
awk '/./ {
  cty=$0;
  getline;
  getline;
  name=$0;
  print "[\""cty"\",\""name"\"],";
} ' county_tbl_encoded.txt > county_def.txt
