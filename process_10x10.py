"""
the script is the sibling of `run_10x10.py`


Collects *.nodal.h5 results from the 4 BWR histories (control in
at void fractions 0, 40, and 70, and control out at a void fraction
of 40) from all 301 realizations, converts each to a DataFrame with
polaris_to_dataframe(), and then exports one big CSV + Parquet of 
nominal values and residuals for use in PERSEUS.
"""

import argparse
import json
import sys
from pathlib import Path

import pandas as pd
from tqdm.auto import tqdm

from polaris_to_df import polaris_to_dataframe, _normalize_branch_map


# ----------------------------------------------------------------------
# defaults (override on CLI)
# ----------------------------------------------------------------------
DEFAULT_HISTORIES = [
    "control",
    "void_frac_00",
    "void_frac_40",
    "void_frac_70",
]
DEFAULT_H5_DIR        = Path(".")
DEFAULT_BRANCHMAP_DIR = Path("branch_maps")
DEFAULT_OUT_PREFIX    = "all_histories"


# ----------------------------------------------------------------------
def load_branch_map(path: Path):
    """Read <history>.json and coerce keys to int."""
    with open(path) as fp:
        bm = json.load(fp)
    return _normalize_branch_map(bm)


def main():
    ap = argparse.ArgumentParser(
        description="Aggregate Polaris *.nodal.h5* files from many histories "
                    "and Sampler indices into a single CSV / Parquet."
    )
    ap.add_argument("--histories", nargs="+", default=DEFAULT_HISTORIES,
                    help="list of base names as used by make_inputs.py")
    ap.add_argument("--h5-dir", default=DEFAULT_H5_DIR, type=Path,
                    help="where the .nodal.h5 files live")
    ap.add_argument("--branch-map-dir", default=DEFAULT_BRANCHMAP_DIR, type=Path,
                    help="directory that contains <history>.json branch maps")
    ap.add_argument("--out-prefix", default=DEFAULT_OUT_PREFIX,
                    help="basename for the output files (no extension)")
    ap.add_argument("--samples", nargs=2, type=int, metavar=("FIRST","LAST"),
                    default=(0, 300), help="range of sample_idx to read (inclusive)")
    args = ap.parse_args()

    first, last = args.samples
    if first < 0 or last < first:
        sys.exit("ERROR: --samples FIRST LAST must satisfy 0 ≤ FIRST ≤ LAST")

    all_frames = []
    missing = 0

    # ------------------------------------------------------------------
    # iterate histories
    # ------------------------------------------------------------------
    for hist in args.histories:
        bm_path = args.branch_map_dir / f"{hist}.json"
        if not bm_path.exists():
            sys.exit(f"Branch‑map file {bm_path} not found")

        branch_map = load_branch_map(bm_path)

        for idx in tqdm(range(first, last + 1),
                        desc=f"{hist}", unit="sample"):
            tag = f"{idx:05d}"
            h5_file = args.h5_dir / f"bwr_{hist}_{tag}.nodal.h5"

            if not h5_file.exists():
                missing += 1
                continue  # silently skip; or log if you prefer

            df = polaris_to_dataframe(h5_file,
                                      branch_map=branch_map,
                                      sample_idx=idx)
            df["history"] = hist
            all_frames.append(df)

    if not all_frames:
        sys.exit("ERROR: no HDF5 files were found; nothing to do.")

    full_df = pd.concat(all_frames, ignore_index=True)

    csv_path     = f"{args.out_prefix}.csv"
    parquet_path = f"{args.out_prefix}.parquet"

    full_df.to_csv(csv_path, index=False)
    full_df.to_parquet(parquet_path, index=False)  # requires pyarrow ≥1.0

    print(f"\n✓ Wrote {len(full_df):,} rows → {csv_path} and {parquet_path}")
    if missing:
        print(f"⚠  {missing} .nodal.h5 files were missing and skipped.")


if __name__ == "__main__":
    main()
