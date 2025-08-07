#!/usr/bin/env python3
"""
polaris_to_df.py
Read a Polaris (SCALE) lattice HDF5 result, including pin-power-factors (PPFs),
and return / write a Pohjantahti-style pandas DataFrame.

Changes v2
-----------
* NEW:  PPFs support – flattens an (nrow, ncol) array to columns
        ppf_r<i>_c<j>  (1-based indices to match human convention).
* NEW:  retains `depl_idx`, `branch_idx`, `homog_idx` columns so tests may
        resolve the exact H5 path later.
* Generalised to any (row, col) size: works for single pin (1x1) and 3x3, 10x10.
"""

import re
import argparse
import json
from pathlib import Path
import random

import h5py
import numpy as np
import pandas as pd


# --------------------------------------------------------------------
# 0. Case-specific metadata (edit per study)
# --------------------------------------------------------------------
BRANCH_MAP = {
    # branchIdx :  FT [°C],  VF [%], CB [ppm]
     0: {"FT": 900.0, "VF": 40.0, "CB": 0.0},  # base
     1: {"FT": 800.0, "VF": 40.0, "CB": 0.0},
     2: {"FT": 1000., "VF": 40.0, "CB": 0.0},
     3: {"FT": 900.0, "VF": 0.0,  "CB": 0.0},
     4: {"FT": 900.0, "VF": 70.0, "CB": 0.0},
}

# default few-group paramaters to read
XS_KEYS = (
    "capture", "fission", "total", "diffcoef",
    "transfer",                     # 3 × 3
)

_RE_SPG = re.compile(r"^(\d+)_(\d+)_(\d+)$")    # state-point group name

# --------------------------------------------------------------------
# 1. low-level helpers
# --------------------------------------------------------------------
def _normalize_branch_map(d):
    """
    Return a copy whose keys are *integers*.
    Allows JSON files that inevitably use string keys.
    """
    return {int(k): v for k, v in d.items()}

def _iter_state_groups(h5):
    """Yield (name, depl, br, hom) for every recognised state-point group."""
    for name in h5:
        m = _RE_SPG.match(name)
        if m:
            yield name, *(int(x) for x in m.groups())


def _flatten_xs(grp, keys):
    out = {}
    G = grp["xs"]["capture"].shape[0]           # no. energy groups

    for key in keys:
        if key == "transfer":
            mat = grp["xs"][key][()]            # (G, G)
            for g_to in range(G):
                for g_from in range(G):
                    out[f"{key}_{g_to+1}_{g_from+1}"] = float(mat[g_to, g_from])
        else:
            vec = np.asarray(grp["xs"][key])
            for g, v in enumerate(vec, 1):
                out[f"{key}_{g}"] = float(v)
    return out


def _flatten_ppfs(ppf):
    """Return dict with column names ppf_<i>_<j>."""
    nrow, ncol = ppf.shape
    return {f"ppf_{i}_{j}": float(ppf[i, j])
            for i in range(nrow) for j in range(ncol)}


# --------------------------------------------------------------------
# 2. main API
# --------------------------------------------------------------------
def polaris_to_dataframe(h5file, branch_map=BRANCH_MAP,
                         xs_keys=XS_KEYS, sample_idx=0):
    branch_map = _normalize_branch_map(branch_map)
    rows = []

    with h5py.File(h5file, "r") as f:
        default_state = branch_map[0]

        for gname, dep, br, hom in _iter_state_groups(f):
            grp = f[gname]
            state = branch_map.get(br)
            if state is None:
                raise KeyError(f"branch_idx={br} missing from branch_map")

            rec = dict(
                sample_idx=sample_idx,
                depl_idx=dep,
                branch_idx=br,
                homog_idx=hom,
                burnup=float(np.asarray(grp["burnup"])[-1]),
                default_FT=default_state["FT"],
                default_VF=default_state["VF"],
                default_CB=default_state["CB"],
                branch_FT=state["FT"],
                branch_VF=state["VF"],
                branch_CB=state["CB"],
                kinf=float(grp["kinf"][()]),
            )

            # XS vectors / matrices
            rec.update(_flatten_xs(grp, xs_keys))

            # Pin-power factors (optional)
            if "PPFs" in grp:
                rec.update(_flatten_ppfs(np.asarray(grp["PPFs"])))

            rows.append(rec)

    df = pd.DataFrame(rows).sort_values(
        ["depl_idx", "branch_idx", "homog_idx"]).reset_index(drop=True)
    return df


# --------------------------------------------------------------------
# 3. CLI entry-point
# --------------------------------------------------------------------
def _main():
    ap = argparse.ArgumentParser(
        description="Convert Polaris H5 to Pohjantahti-style CSV (with PPFs)")
    ap.add_argument("h5file")
    ap.add_argument("-o", "--outfile",
                    help="output CSV (default same basename)")
    ap.add_argument("--branch-map",
                    help="JSON overriding the hard-coded BRANCH_MAP")
    ap.add_argument("--xs", nargs="+",
                    help="space-separated list of XS keys to extract")
    args = ap.parse_args()

    branch_map = (json.load(open(args.branch_map))
                  if args.branch_map else BRANCH_MAP)
    xs_keys = tuple(args.xs) if args.xs else XS_KEYS

    df = polaris_to_dataframe(args.h5file, branch_map, xs_keys)
    out = Path(args.outfile or Path(args.h5file).with_suffix(".csv"))
    df.to_csv(out, index=False)
    print(f"{len(df):,} state-points → {out}")
    return df
if __name__ == "__main__":
    df = _main()
