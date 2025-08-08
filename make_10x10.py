# Make 10x10 inputs in ./inp

_temp_nomi_ = """=shell
  ln -sf $BWRLIBS/mg_bwr mg_bwr
end
"""

_temp_smpl_ = """=shell
  ln -sf $BWRLIBS/mg_bwr ft89f001
  ln -sf $BWRLIBS/Sample{0} ft87f001
end
=clarolplus
  in=89
  out=88
  var=87
  isvar=10
  bond=yes
  sam={0}
end
=shell
  mv ft88f001 mg_bwr
  unlink ft87f001
  unlink ft89f001
  mv ft10f001 crawdadPerturbMGLib
end
"""

input_bases = ["control", "void_frac_00", "void_frac_40", "void_frac_70"]

for base in input_bases:
    with open(f"bwr_{base}_base.inp", "r") as f:
        s_base = f.read()

    # nominal case
    with open(f"inp/bwr_{base}_00000.inp", "w") as f:
        f.write(_temp_nomi_ + s_base)

    # sampled cases
    for i in range(1, 301):
        sample_id = f"{i:05d}"  # zero-padded to 5 digits
        s = _temp_smpl_.format(i) + s_base
        with open(f"inp/bwr_{base}_{sample_id}.inp", "w") as f:
            f.write(s)
