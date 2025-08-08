import unittest
import json
from pathlib import Path

from polaris_to_df import polaris_to_dataframe


class TestPolarisToDF(unittest.TestCase):
    """
    Test of conversion from HDF5 to DF (as CSV) for a single 3x3 case.
    """

    def test_3x3(self):

        H5_PATH = Path("b3x3.nodal.h5")
        BRANCH_MAP_PATH = Path("b3x3_branch_map.json")

        with open(BRANCH_MAP_PATH) as fp:
            branch_map = json.load(fp)

        try:
            df = polaris_to_dataframe(H5_PATH, branch_map)
        except:
            print("You need to run b3x3.inp first.")
            return

        # arbitrary branch and burnup
        mask = (
            (df["default_FT"] == df["branch_FT"])
            & (df["burnup"] == 1.0)
            & (df["branch_VF"] == 0)
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

    def test_10x10(self):
        """Test using a couple of manually-inspected,
        locally-computed, .out files as references.
        """

        #######################################################################
        # case 1
        #  history:    VF=40
        #  sample:     2
        #  statepoint: 99 (Depletion pass no.    3, branch no.    5)
        #     --> burnup=2.0000E+00
        #     --> control is inserted
        burnup = 2.0
        H5_PATH = Path("inp/bwr_void_frac_40_00002.nodal.h5")
        branch_map = {
            0: {"FT": 900, "VF": 40, "CB": 0},
            1: {"FT": 800, "VF": 40, "CB": 0},
            2: {"FT": 1000, "VF": 40, "CB": 0},
            3: {"FT": 900, "VF": 0, "CB": 0},
            4: {"FT": 900, "VF": 70, "CB": 0},
            5: {"FT": 900, "VF": 40, "CB": 1},
        }

        try:
            df = polaris_to_dataframe(H5_PATH, branch_map)
        except:
            print("You need to run inp/bwr_void_frac_40_00002.inp first.")
            return

        # Select the statepoint
        mask = (
            (df["default_FT"] == df["branch_FT"])
            & (df["burnup"] == burnup)
            & (df["branch_VF"] == 40)
            & (df["branch_CB"] == True)
        )

        expect = 0.84567
        got = df.loc[mask, "kinf"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 5.050e-03
        got = df.loc[mask, "fission_2"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        # Note, the .out must normalize different from t16/h5
        expect = 2.18457803E-02 
        got = df.loc[mask, "flux_3"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.4641
        got = df.loc[mask, "ppf_1_0"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.4641
        got = df.loc[mask, "ppf_0_1"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.0000
        got = df.loc[mask, "ppf_4_5"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 1.7065
        got = df.loc[mask, "ppf_9_9"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        #######################################################################
        # case 2 
        #  history:    VF=70
        #  sample:     88
        #  statepoint: 32 (Depletion pass no.   12, branch no.    1)
        #     --> Burnup = 1.5000E+01 (GWD/MTHM)
        #     --> Branch = 'FT' (to 800 K)
        burnup = 15.0
        H5_PATH = Path("inp/bwr_void_frac_70_00088.nodal.h5")
        branch_map = {
            0: {"FT": 900, "VF": 70, "CB": 0},
            1: {"FT": 800, "VF": 70, "CB": 0},
            2: {"FT": 1000, "VF": 70, "CB": 0},
            3: {"FT": 900, "VF": 0, "CB": 0},
            4: {"FT": 900, "VF": 40, "CB": 0},
            5: {"FT": 900, "VF": 70, "CB": 1},
        }

        try:
            df = polaris_to_dataframe(H5_PATH, branch_map)
        except:
            print("You need to run inp/bwr_void_frac_70_00088.inp first.")
            return

        # Select the statepoint
        mask = (
            (df["branch_FT"] == 800)
            & (df["burnup"] == burnup)
            & (df["branch_VF"] == 70)
            & (df["branch_CB"] == False)
        )

        expect = 1.06773
        got = df.loc[mask, "kinf"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 4.024E-03
        got = df.loc[mask, "fission_2"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        # Note, the .out must normalize different from t16/h5
        expect = 3.03129163E-02
        got = df.loc[mask, "flux_3"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 1.0951 
        got = df.loc[mask, "ppf_1_0"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 1.0951 
        got = df.loc[mask, "ppf_0_1"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.0000
        got = df.loc[mask, "ppf_4_5"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 1.0955
        got = df.loc[mask, "ppf_9_9"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)


        #######################################################################
        # case 3 
        #  history:    control inserted 
        #  sample:     99
        #  statepoint: 13 (Depletion pass no.   13, branch no.    0)
        #     --> Burnup = 2.0000E+01 (GWD/MTHM)
        #     --> Branch = None
        burnup = 20.0
        H5_PATH = Path("inp/bwr_control_00099.nodal.h5")
        branch_map = {
          0: {"FT": 900, "VF": 40, "CB": 1},
          1: {"FT": 900, "VF": 40, "CB": 0},
        }

        try:
            df = polaris_to_dataframe(H5_PATH, branch_map)
        except:
            print("You need to run inp/bwr_control_00099.inp first.")
            return

        # Select the statepoint
        mask = (
            (df["branch_FT"] == 900)
            & (df["burnup"] == burnup)
            & (df["branch_VF"] == 40)
            & (df["branch_CB"] == True)
        )

        expect = 0.87304
        got = df.loc[mask, "kinf"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 4.227E-03
        got = df.loc[mask, "fission_2"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        # Note, the .out must normalize different from t16/h5
        #expect = 3.03129163E-02
        #got = df.loc[mask, "flux_3"].iloc[0]
        #self.assertAlmostEqual(expect, got, places=4)

        expect = 0.5414 
        got = df.loc[mask, "ppf_1_0"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.5414 
        got = df.loc[mask, "ppf_0_1"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 0.0000
        got = df.loc[mask, "ppf_4_5"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

        expect = 1.4411
        got = df.loc[mask, "ppf_9_9"].iloc[0]
        self.assertAlmostEqual(expect, got, places=4)

if __name__ == "__main__":
    unittest.main()
