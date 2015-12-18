#for FORMAT in raw prop single; do
#  java -jar bin/SempalaNativeLoader-1.0.jar \
#    --host dbisst02.informatik.privat --database schneidm_sempala \
#    --input /user/schneidm/data/BSBM --output BSBM_${FORMAT} \
#    --format ${FORMAT} --prefix-file prefixes/BSBM_Prefixes.txt \
#    --field-terminator ' ' \
#    --strip-dot --unique
#
#  java -jar bin/SempalaNativeLoader-1.0.jar \
#    --host dbisst02.informatik.privat --database schneidm_sempala \
#    --input /user/schneidm/data/LUBM/ --output LUBM_${FORMAT} \
#    --format ${FORMAT} --prefix-file prefixes/LUBM_Prefixes.txt \
#    --field-terminator ' ' \
#    --unique
#
#  java -jar bin/SempalaNativeLoader-1.0.jar \
#    --host dbisst02.informatik.privat --database schneidm_sempala \
#    --input /user/schneidm/data/SP2Bench/ --output SP2Bench_${FORMAT} \
#    --format ${FORMAT} --prefix-file prefixes/SP2Bench_Prefixes.txt \
#    --field-terminator ' ' \
#    --strip-dot --unique
#
#  java -jar bin/SempalaNativeLoader-1.0.jar \
#    --host dbisst02.informatik.privat --database schneidm_sempala \
#    --input /user/schneidm/data/WatDiv/ --output WatDiv_${FORMAT} \
#    --format ${FORMAT} --prefix-file prefixes/WatDiv_Prefixes.txt \
#    --field-terminator '\t' \
#    --strip-dot --unique
#done


java -jar bin/SempalaNativeLoader-1.0.jar \
  --host dbisst02.informatik.privat --database schneidm_sempala \
  --input /user/schneidm/data/WatDiv1M/ --output WatDiv_single1M \
  --format single --prefix-file prefixes/WatDiv_Prefixes.txt \
  --field-terminator '\t'

