BEGIN { idx=Commit; FS="\t"; RS="\n"; }; 
/./ { gsub(/"/, "\"\""); 
    gsub(/\r/, ""); 
    gsub(/\t/, "\",\""); }; 
idx==Commit { 
      print ";\nINSERT INTO voter_history (county_code, voter_id, election_date, election_type, history_code) VALUES "; \
    print " (\"" $0 "\")" ; idx=0;};
idx!=Commit {
      print " ,(\"" $0 "\")" ; idx=idx+1; next; }; 
END { print ";" };
