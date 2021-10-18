BEGIN { idx=Commit; FS="\t"; RS="\n"; }
/./ { gsub(/"/, "\"\""); 
    gsub(/\r/, ""); 
    gsub(/\t/, "\",\""); }; 
idx!=Commit { 
    print " ,(\"" $0 "\",\"" eDate "\")" ; idx=idx+1; next; } 
idx==Commit { print ";\nINSERT INTO voter (county_code, voter_id, name_last, name_suffix, name_first, name_middle, is_suppressed, res_address1, res_address2, res_city, res_state, res_zipcode, mail_address1, mail_address2, mail_address3, mail_city, mail_state, mail_zipcode, mail_country, gender, race, birth_date, registration_date, party_affiliation, precinct, precinct_group, precinct_split, precinct_suffix, voter_status, cong_dist, house_dist, senate_dist, county_comm_dist, school_brd_dist, day_phone_area_code, day_phone_number, day_phone_ext, email, extract_date) VALUES "; 
    print " (\"" $0 "\",\"" eDate "\")" ; idx=0;} 
END { print ";" } 

