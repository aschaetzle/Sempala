#!/bin/bash

java -jar bin/SempalaNativeLoader-1.0.jar \
  -i /user/schneidm/data/watdiv/ \
  -h dbisst02.informatik.privat \
  -d schneidm_sempala \
  -o test_table \
  -f prop \
  -P Prefixes.txt \
  -s
