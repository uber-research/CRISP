# Run unit tests.
python3 -m pytest

# Run larger tests.
for i in "test_cases"/*.json
do
  rm -f test_cases/*.html test_cases/*cct*
  indir=`mktemp -d`
  outdir=`mktemp -d`
  cp $i $indir
  python3 process.py -a O1 -s S1 -t "$indir" -o "$outdir"
done
