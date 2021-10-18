#!/bin/bash

#merge_extracts.sh

# stop on error
set -e
if [ -n "$DEBUG" ]; then
    set -x
fi

export TMPDIR=`pwd`

masterDB="fl_master.gpkg"

usage() {
	echo "usage:
	"
	exit 1
}

copy_dvd() {
    #sudo ls .
    devx="`ls -l /dev/disk/by-label | grep '/sr0' | awk '/./ { print $9; }'`"
    x=`udisksctl mount -b /dev/sr0`
    sleep 1
    j=`printf $devx`
    j=`df -h -t udf -l --output=target | tail -n 1`
    k=`basename "$j" | sed -e 's/ /_/g'`

    echo "copy from $j to $k"
    if [ -r "$j" ]; then
        rsync -rv "$j"/ dvd/$k --progress --size-only
    fi
    eject /dev/sr0

    chmod -R a+r dvd/$k
    chmod a+x dvd/$k

    echo "$k"
}

merge_extract() {
    i="$1"
    j="${i:10:4}-${i:14:2}-${i:16:2}"

    echo "
    PRAGMA SYNCHRONOUS=off;
    SELECT CURRENT_TIMESTAMP;
    CREATE TEMP TABLE timing_tmp AS SELECT CURRENT_TIMESTAMP as starttime;

    ATTACH DATABASE '$1' as m;

    CREATE INDEX IF NOT EXISTS m.voter__voter_id__ind on voter(voter_id);
    CREATE INDEX IF NOT EXISTS m.voter_history__voter_id__ind on voter_history(voter_id);

    -- set the status for purged voters
    -- UPDATE voter as v SET voter_status = 'PURGED $j' WHERE NOT EXISTS (SELECT 1 FROM m.voter as mv WHERE v.voter_id = mv.voter_id);
    -- SELECT 'voter purges', changes();

    -- add new voters and voter histories
    INSERT into voter SELECT *
        FROM m.voter as mv 
	WHERE NOT EXISTS (SELECT 1 FROM voter v WHERE mv.voter_id = v.voter_id);
    SELECT 'voter inserts', changes();

    INSERT INTO voter_history SELECT * 
        FROM m.voter_history mv 
	WHERE NOT EXISTS (SELECT 1 FROM voter_history v WHERE mv.voter_id = v.voter_id);
    SELECT 'voter_history inserts', changes();

    UPDATE merge_log SET merge_status = 'ok' WHERE filename = '$1';

    SELECT CURRENT_TIMESTAMP, starttime, 'runtime' ,(strftime('%s',CURRENT_TIMESTAMP) - strftime('%s',starttime))/60.0, 'minutes' FROM timing_tmp;
    " | tee -a sql/build.sql | sqlite3 $masterDB

    sync
}

list_extracts() {
    ls  fl_voters_[12]*.gpkg
}

main_merge() {
    i="$1"
    j="${i:10:4}-${i:14:2}-${i:16:2}"
    if [ -n "$2" ]; then
      cp -p "$i" $masterDB
    fi

    (echo "PRAGMA SYNCHRONOUS=off;

        CREATE TABLE IF NOT EXISTS voter (county_code text(3), voter_id text(10), 
	    name_last text(30), name_suffix text(5), name_first text(30), name_middle text(30), is_suppressed text(1), 
	    res_address1 text(50), res_address2 text(40), res_city text(40), res_state text(2), res_zipcode text(10), 
	    mail_address1 text(40), mail_address2 text(40), mail_address3 text(40), mail_city text(40), mail_state text(2), mail_zipcode text(12), 
	    mail_country text(40), gender text(1), race text(1), birth_date text(10), registration_date text(10), party_affiliation text(3), 
	    precinct text(6), precinct_group text(3), precinct_split text(6), precinct_suffix text(3), voter_status text(3), 
	    cong_dist text(3), house_dist text(3), senate_dist text(3), county_comm_dist text(3), school_brd_dist text(2), 
	    day_phone_area_code text(3), day_phone_number text(7), day_phone_ext text(4), 
	    email text(100), extract_date text(10));
    
        CREATE TABLE IF NOT EXISTS voter_history
        (county_code text(3), voter_id text(10), election_date text(10), election_type text(10), history_code text(1));
    "

    echo "CREATE TABLE IF NOT EXISTS merge_log (fid INTEGER PRIMARY KEY AUTOINCREMENT,
       extract_date  DATE,
       filename TEXT UNIQUE,
       merge_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       merge_status TEXT);

       CREATE INDEX IF NOT EXISTS voter__voter_id__ind on voter(voter_id);
       CREATE INDEX IF NOT EXISTS voter_history__voter_id__ind on voter_history(voter_id);

       -- add the stub version to the log
       INSERT OR REPLACE INTO merge_log (extract_date, filename, merge_status) VALUES ('$j', '$1', 'ok');

       ") | tee -a sql/build.sql | sqlite3 $masterDB
	sync

    (
    for i in `list_extracts`; do
	    j="${i:10:4}-${i:14:2}-${i:16:2}"
	    echo "INSERT OR REPLACE INTO merge_log (extract_date, filename) SELECT '$j', '$i' WHERE NOT EXISTS (SELECT 1 FROM merge_log WHERE filename = '$i');"
    done  ) | tee -a sql/build.sql | sqlite3 $masterDB

    import_support
    # go in reverse order assuming that newer data is better
    merge_list=`echo "SELECT filename FROM merge_log WHERE merge_status is NULL ORDER BY filename DESC" | sqlite3 $masterDB`
    echo $merge_list
    for m in $merge_list; do
        merge_extract $m
	sync
    done
    echo ".headers yes
    SELECT * FROM merge_log order by extract_date" | sqlite3 $masterDB 
}

history_update_awk_stmt() {
    return '\
BEGIN { idx=Commit; FS="\t"; RS="\n"; }; \
/./ { gsub(/"/, "\"\""); \
    gsub(/\r/, ""); \
    gsub(/\t/, "\",\""); }; \
idx==Commit { \
      print ";\nINSERT INTO voter_history (county_code, voter_id, election_date, election_type, history_code) VALUES "; \
    print " (\"" $0 "\")" ; idx=0;}; \
idx!=Commit { \
      print " ,(\"" $0 "\")" ; idx=idx+1; next; }; \
END { print ";" };'
}

voter_update_awk_stmt() {
    return '\
BEGIN { idx=Commit; FS="\t"; RS="\n"; }\
/./ { gsub(/"/, "\"\""); \
    gsub(/\r/, ""); \
    gsub(/\t/, "\",\""); }; \
idx!=Commit { \
    print " ,(\"" $0 "\",\"" eDate "\")" ; idx=idx+1; next; } \
idx==Commit { print ";\nINSERT INTO voter (county_code, voter_id, name_last, name_suffix, name_first, name_middle, is_suppressed, res_address1, res_address2, res_city, res_state, res_zipcode, mail_address1, mail_address2, mail_address3, mail_city, mail_state, mail_zipcode, mail_country, gender, race, birth_date, registration_date, party_affiliation, precinct, precinct_group, precinct_split, precinct_suffix, voter_status, cong_dist, house_dist, senate_dist, county_comm_dist, school_brd_dist, day_phone_area_code, day_phone_number, day_phone_ext, email, extract_date) VALUES "; \
    print " (\"" $0 "\",\"" eDate "\")" ; idx=0;} \
END { print ";" } \
'
}

import_history() {
    zF="$1"
    j=${zF:27:8}
    if [ ! -s "$zF" ]; then
        return 1
    fi

    (echo "PRAGMA SYNCHRONOUS=off; 
        CREATE TABLE IF NOT EXISTS voter_history
        (county_code text(3), voter_id text(10), election_date text(10), election_type text(10), history_code text(1));
	"

    # transform the tab separated data into INSERTS	
    unzip -p $zF | awk  -v Commit=5001  -f history_update.awk 
    
    ) | sqlite3 $iDB

# for debugging 
## | tee sql/import_vh_stms.sql | sqlite3 $iDB

}

import_voter() {
    zR="$1"
    j="$udt"
    (echo "PRAGMA SYNCHRONOUS=off;

CREATE TABLE IF NOT EXISTS voter (county_code text(3), voter_id text(10), name_last text(30), name_suffix text(5), name_first text(30), name_middle text(30), is_suppressed text(1), res_address1 text(50), res_address2 text(40), res_city text(40), res_state text(2), res_zipcode text(10), mail_address1 text(40), mail_address2 text(40), mail_address3 text(40), mail_city text(40), mail_state text(2), mail_zipcode text(12), mail_country text(40), gender text(1), race text(1), birth_date text(10), registration_date text(10), party_affiliation text(3), precinct text(6), precinct_group text(3), precinct_split text(6), precinct_suffix text(3), voter_status text(3), cong_dist text(3), house_dist text(3), senate_dist text(3), county_comm_dist text(3), school_brd_dist text(2), day_phone_area_code text(3), day_phone_number text(7), day_phone_ext text(4), email text(100), extract_date text(10));
    "

    # transform the tab separated data into INSERTS	
    unzip -p $zR | awk -v eDate="$j" -v Commit=5001 -f voter_update.awk
    
    ) | sqlite3 $iDB


# ====
# for debugging 
## | tee sql/import_v_stms.sql | sqlite3 $iDB

}

import_month() {
    for ix in import*sql; do
        if [ -r "$ix" ]; then
            rm "$ix" | :
        fi
    done
    if [ -r "$iDB" ]; then
        rm $iDB | :
    fi

    for i in dvd/${1}/*History*zip; do
        if [ -s "$i" ]; then
            time import_history "$i"
            echo "done import history into $iDB"
            break
        fi
    done
    # brute force handling of recent naming changes

    for i in dvd/${1}/*Registration*zip dvd/${1}/*Detail*zip; do
        if [ -s "$i" ]; then
            time import_voter "$i"
            echo "done import voters into $iDB"
            break
        fi
    done
    time import_support

    echo "done import into $iDB"
}

import_support() {
    cat sql/support_tables.sql | sqlite3 $iDB
    echo "done build support tables"
}

main_reimport() {
    for i in `ls dvd/*/Voter_History*zip | sort | head -n 1`; do
	if [ -s "$i" ]; then
            time import_history $i
	fi
	:
    done

    for i in `ls dvd/*/Voter_Registration*zip dvd/*/Voter*Detail*zip | sort | head -n 1`; do
	if [ -s "$i" ]; then
           time import_voter $i
	fi
    done

    echo "end main_reimport"
}

main_run() {

if [ "$1" == "--import" ]; then
    dt=`copy_dvd`
    shift
else
    dt=$1
fi

idt=`echo ${dt} | sed 's/_/ /g'`
udt=`date -d "$idt" -I`
iDB="data/db/"`date -d "$idt" "+import_%Y%m%d.db"`

if [ -n "$dt" ]; then
    import_month "$dt"
    echo "done Import Monthly file $dt"
else
    rm fl_master.gpkg sql/import_v_stms.sql sql/import_vh_stms.sql import.db | :

    iDB=master.db
    main_reimport
    main_merge $dt $1
fi
}

main_run $@

