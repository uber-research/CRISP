# Trace smoke tests (process.py over test_cases/*.json). Unit tests: pytest or scripts/ci-local.sh.
for i in "test_cases"/*.json
do
  rm -f test_cases/*.html test_cases/*cct*
  indir=`mktemp -d`
  outdir=`mktemp -d`
  cp $i $indir
  python3 process.py -a O1 -s S1 -t "$indir" -o "$outdir"
done
