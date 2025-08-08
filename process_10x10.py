"""
Process the h5 files generated in ./inp.


Collects *.nodal.h5 results from the 4 BWR histories (control in
at void fractions 0, 40, and 70, and control out at a void fraction
of 40) from all 301 realizations, converts each to a DataFrame with
polaris_to_dataframe(), and then exports one big CSV + Parquet of 
nominal values and residuals for use in PERSEUS.
"""

import pandas as pd
from pathlib import Path
from polaris_to_df import polaris_to_dataframe

branch_maps = {
    "bwr_control": {
          0: {"FT": 900, "VF": 40, "CB": 1},
          1: {"FT": 900, "VF": 40, "CB": 0},
    },
    "bwr_void_frac_00": {
          0: {"FT":  900, "VF":  0, "CB": 0},
          1: {"FT":  800, "VF":  0, "CB": 0},
          2: {"FT": 1000, "VF":  0, "CB": 0},
          3: {"FT":  900, "VF": 40, "CB": 0},
          4: {"FT":  900, "VF": 70, "CB": 0},
          5: {"FT":  900, "VF":  0, "CB": 1}
    },
    "bwr_void_frac_40": {
          0: {"FT":  900, "VF": 40, "CB": 0},
          1: {"FT":  800, "VF": 40, "CB": 0},
          2: {"FT": 1000, "VF": 40, "CB": 0},
          3: {"FT":  900, "VF":  0, "CB": 0},
          4: {"FT":  900, "VF": 70, "CB": 0},
          5: {"FT":  900, "VF": 40, "CB": 1}
    },
    "bwr_void_frac_70": {
          0: {"FT":  900, "VF": 70, "CB": 0},
          1: {"FT":  800, "VF": 70, "CB": 0},
          2: {"FT": 1000, "VF": 70, "CB": 0},
          3: {"FT":  900, "VF":  0, "CB": 0},
          4: {"FT":  900, "VF": 40, "CB": 0},
          5: {"FT":  900, "VF": 70, "CB": 1}
    }
}

if __name__ == "__main__":
    all_frames = []
    for hist in branch_maps:
        branch_map = branch_maps[hist]
        for i in range(0, 101):
            sample_id = f"{i:05d}"  # zero-padded to 5 digits
            h5_file = f"inp/{hist}_{sample_id}.nodal.h5"
            print("converting " + h5_file)
            #assert h5_file.exists(), h5_file+" not found!"
            df = polaris_to_dataframe(h5_file,
                                      branch_map=branch_map,
                                      sample_idx=i)
            df["history"] = hist
            all_frames.append(df)
    full_df = pd.concat(all_frames, ignore_index=True)

    # Sort 
    cols = [
        "default_FT",
        "default_VF",
        "default_CB",
        "branch_FT",
        "branch_VF",
        "branch_CB",
        "burnup",
        "sample_idx",
    ]
    full_df = full_df.sort_values(by=cols, ascending=True).reset_index(drop=True)
    csv_path     = f"bwr_10x10.csv"
    parquet_path = f"bwr_10x10.parquet"

    full_df.to_csv(csv_path, index=False)
    #full_df.to_parquet(parquet_path, index=False)  # requires pyarrow ≥1.0

    print(f"\n✓ Wrote {len(full_df):,} rows → {csv_path} and {parquet_path}")

