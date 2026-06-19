# Smoke tests: run process_trace over each test_cases/*.json file.
# Unit tests: pytest or scripts/ci-local.sh.
for i in "test_cases"/*.json
do
  rm -f test_cases/*.html test_cases/*cct*
  indir=`mktemp -d`
  outdir=`mktemp -d`
  cp $i $indir
  python3 -m crisp.process_trace -a O1 -s S1 -i "$indir" -o "$outdir"
done
