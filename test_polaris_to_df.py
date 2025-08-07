import unittest
import json
from pathlib import Path

from polaris_to_df import polaris_to_dataframe, _normalize_branch_map

H5_PATH = Path("b3x3.nodal.h5")
BRANCH_MAP_PATH = Path("b3x3_branch_map.json")

class TestPolarisToDF(unittest.TestCase):
    """
    Test of conversion from HDF5 to DF (as CSV) for single 3x3 case.
    """

    def test_3x3(self):
        with open(BRANCH_MAP_PATH) as fp:
            branch_map = json.load(fp)

        try:
            df = polaris_to_dataframe(H5_PATH, branch_map)
        except:
            print("You need to run b3x3.inp first.")
            return 
        
        # arbitrary branch and burnup
        mask = (
            (df["default_FT"] == df["branch_FT"]) &
            (df["burnup"]     == 1.0) &
            (df["branch_VF"]  == 0)
        )

        # kinf
        expect = 0.8933815360069275
        got = df.loc[mask, "kinf"].iloc[0]
        self.assertAlmostEqual(expect, got, places=10)

        # capture_2
        expect = 0.020019613206386566
        got = df.loc[mask, "capture_2"].iloc[0]
        self.assertAlmostEqual(expect, got, places=10)

        # transfer_2_3
        expect = 0.003127276198938489
        got = df.loc[mask, "transfer_2_3"].iloc[0]
        self.assertAlmostEqual(expect, got, places=10)

        # ppf_1_0
        expect = 0.9349284768104553
        got = df.loc[mask, "ppf_1_0"].iloc[0]
        self.assertAlmostEqual(expect, got, places=10)

        # ppf_1_1
        expect = 0.9307556748390198
        got = df.loc[mask, "ppf_0_1"].iloc[0]
        self.assertAlmostEqual(expect, got, places=10)

if __name__ == "__main__":
    unittest.main()
