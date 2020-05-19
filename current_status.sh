ls Final_Json | tqdm --total=295 --desc='TOTAL'|wc -l
grep -E '999 seconds' debug.log | tqdm --total=295 --desc='ERROR'|wc -l