#!/bin/bash

set -e
set -x 
export DEBUG=""

redo_quickload() {
export TMPDIR=$HOME/tmp
for i in data/db/import_*.db; do
    dt=${i:15:8}
    rx=`date -d"$dt" "+%b_%d_%Y"`
    if [ -r "dvd/${rx}" ]; then
        echo "import $rx"
        bash quick_import.sh $rx
    fi
done

}

quickload_all() {
export TMPDIR=$HOME/tmp
for i in dvd/*; do
    if [ ! -d $i ]; then
        continue
    fi
    dt=`basename $i | tr '_' ' '`
    irx=`date -d"$dt" "+%Y%m%d"`
    rx=`date -d"$dt" "+%b_%d_%Y"`

    # do not reimport existing DBs
    if [ -r "dvd/${rx}" -a ! -r "data/db/import_${irx}.db" ]; then
        echo "import $rx"
         
        bash quick_import.sh "$rx"
    fi
done

}

merge_db() {
    export TMPDIR=$HOME/tmp

    # fuse-zip -r archivetest.zip /mnt
     
}

merge_all() {
  :

}

update_db() {
export TMPDIR=$HOME/tmp
for i in data/db/import_*.db; do
    dt=${i:15:8}
    rx=`date -d"$dt" -I`
    echo "import $rx"
    echo "PRAGMA SYNCHRONOUS=off;
-- enable the sqlite3 shell timer
.timer on
    UPDATE VOTER SET extract_date = '$rx'
    WHERE extract_date <> '$rx';
    SELECT 'Changed Rows', changes();
    VACUUM;" | tee fixer.sql | sqlite3 $i
done

}

usage() {
    echo "usage:
     bash reimport.sh [-update] [-reload]
    "

}

case "$1" in
-update)
    update_db
    ;; 
-reload)
    quickload_all
    ;;
*)
    usage
esac
