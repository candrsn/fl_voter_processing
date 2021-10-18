#!/bin/bash

#merge_extracts.sh

# stop on error
set -e

export TMPDIR=`pwd`

masterDB="fl_master.gpkg"

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
    " | tee -a build.sql | sqlite3 $masterDB

    sync
}

list_extracts() {
    ls  fl_voters_[12]*.gpkg
}

main() {
    i="$1"
    j="${i:10:4}-${i:14:2}-${i:16:2}"
    if [ -n "$2" ]; then
      cp -p "$i" $masterDB
    fi

    echo "CREATE TABLE IF NOT EXISTS merge_log (fid INTEGER PRIMARY KEY AUTOINCREMENT,
       extract_date  DATE,
       filename TEXT UNIQUE,
       merge_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
       merge_status TEXT);

       CREATE INDEX IF NOT EXISTS voter__voter_id__ind on voter(voter_id);
       CREATE INDEX IF NOT EXISTS voter_history__voter_id__ind on voter_history(voter_id);

       -- add the stub version to the log
       INSERT OR REPLACE INTO merge_log (extract_date, filename, merge_status) VALUES ('$j', '$1', 'ok');

       " | tee -a build.sql | sqlite3 $masterDB
	sync

    (
    for i in `list_extracts`; do
	    j="${i:10:4}-${i:14:2}-${i:16:2}"
	    echo "INSERT OR REPLACE INTO merge_log (extract_date, filename) SELECT '$j', '$i' WHERE NOT EXISTS (SELECT 1 FROM merge_log WHERE filename = '$i');"
    done  ) | tee -a build.sql | sqlite3 $masterDB

    # go in reverse order assuming that newer data is better
    merge_list=`echo "SELECT filename FROM merge_log WHERE merge_status is NULL ORDER BY filename DESC" | sqlite3 $masterDB`
    echo $merge_list
    for m in $merge_list; do
        merge_extract $m
	sync
    done
    echo ".headers on;
    SELECT * FROM merge_log order by extract_date" | sqlite3 $masterDB 
}


main $1 $2


