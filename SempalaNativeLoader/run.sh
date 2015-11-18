java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/BSBM --output BSBM_raw \
  --format raw --prefix-file prefixes/BSBM_Prefixes.txt \
  --field-terminator ' ' \
  --strip-dot --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/LUBM/ --output LUBM_raw \
  --format raw --prefix-file prefixes/LUBM_Prefixes.txt \
  --field-terminator ' ' \
  --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/SP2Bench/ --output SP2Bench_raw \
  --format raw --prefix-file prefixes/SP2Bench_Prefixes.txt \
  --field-terminator ' ' \
  --strip-dot --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/WatDiv/ --output WatDiv_raw \
  --format raw --prefix-file prefixes/WatDiv_Prefixes.txt \
  --field-terminator '\t' \
  --strip-dot --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/BSBM --output BSBM_prop \
  --format prop --prefix-file prefixes/BSBM_Prefixes.txt \
  --field-terminator ' ' \
  --strip-dot --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/LUBM/ --output LUBM_prop \
  --format prop --prefix-file prefixes/LUBM_Prefixes.txt \
  --field-terminator ' ' \
  --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/SP2Bench/ --output SP2Bench_prop \
  --format prop --prefix-file prefixes/SP2Bench_Prefixes.txt \
  --field-terminator ' ' \
  --strip-dot --unique

java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/WatDiv/ --output WatDiv_prop \
  --format prop --prefix-file prefixes/WatDiv_Prefixes.txt \
  --field-terminator '\t' \
  --strip-dot --unique
