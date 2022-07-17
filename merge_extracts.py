
import sys
import os
import glob
import sqlite3
import time
import logging

logger = logging.getLogger(__name__)

def add_history_from_db(dbFile, mainFile="data/main/history.db"):
    preexisting_db = os.path.exists(mainFile)

    logger.info(f"Adding voter history info from {dbFile} into {mainFile}")

    db = sqlite3.connect(mainFile)
    cur = db.cursor()
    cur.execute(f"ATTACH '{dbFile}' as monthly")
    cur.execute("PRAGMA SYNCHRONOUS=off;")
    assert cur.fetchall() is not None, f"failed to connnect to databases {mainFile}, {dbFile}"

    st_time = time.time()

    if not preexisting_db:
        cur.execute("""
            CREATE TABLE main.voter_history AS SELECT * FROM monthly.voter_history
        """)
        cur.execute("""
            CREATE INDEX main.voter_history__voting_record__ind ON voter_history (voter_id, election_date)""")
        assert cur.fetchall() is not None, f"failed to create initial table for databases {mainFile}, {dbFile}"
        db.commit()
        cur.close()
    else:
        cur.execute("""
            INSERT INTO main.voter_history (county_code, voter_id, election_date, election_type, history_code)
                SELECT county_code, voter_id, election_date, election_type, history_code 
                    FROM monthly.voter_history m 
                    WHERE NOT EXISTS ( SELECT 1 FROM main.voter_history v 
                          WHERE v.voter_id = m.voter_id and
                              v.election_date = m.election_date)
        """)
        assert cur.fetchall() is not None, f"failed to add info to databases {mainFile}, {dbFile}"
        db.commit()
        cur.close()

    logger.info(f"   content added in {time.time() - st_time} seconds")
    db.close()

def merge_histories(workDir):
    dbfiles = sorted(glob.glob(f"{workDir}/*.db"), reverse=True)

    for itm in dbfiles:
        add_history_from_db(dbFile=itm, mainFile="data/main/history.db")

def main(args=[]):
    merge_histories("data/db")
        
if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)

