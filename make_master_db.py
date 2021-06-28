
import sys
import os
import json
import sqlite3
import glob
import subprocess
import logging
import time

def master_tables_ddl(cur):
    # Reference only
    voter_ddl = """
CREATE TABLE IF NOT EXISTS voter (county_code text(3), voter_id text(10), name_last text(30), name_suffix text(5), name_first text(30), name_middle text(30), 
    is_suppressed text(1), 
    res_address1 text(50), res_address2 text(40), res_city text(40), res_state text(2), res_zipcode text(10), 
    mail_address1 text(40), mail_address2 text(40), mail_address3 text(40), mail_city text(40), mail_state text(2), mail_zipcode text(12), mail_country text(40), 
    gender text(1), race text(1), birth_date text(10), registration_date text(10), party_affiliation text(3), 
    precinct text(6), precinct_group text(3), precinct_split text(6), precinct_suffix text(3), 
    voter_status text(3), cong_dist text(3), house_dist text(3), senate_dist text(3), county_comm_dist text(3), school_brd_dist text(2), 
    day_phone_area_code text(3), day_phone_number text(7), day_phone_ext text(4), email text(100), extract_date text(10));
    """

    ddl_cmds = ["""
CREATE TABLE IF NOT EXISTS vp.voter_person (voter_id text(10), name_last text(30), name_suffix text(5), name_first text(30), name_middle text(30),
    gender text(1), race text(1), birth_date text(10), registration_date text(10), party_affiliation text(3),
    day_phone_area_code text(3), day_phone_number text(7), day_phone_ext text(4),
    voter_status text(3), 
    first_instance date, last_instance date );
    ""","""
CREATE TABLE IF NOT EXISTS va.voter_address (voter_id text(10), address_type char(4),
    address1 text(50), address2 text(40), city text(40), state text(2), zipcode text(10),
    first_instance date, last_instance date );
    ""","""
CREATE TABLE IF NOT EXISTS vd.voter_districts (voter_id text(10),
    precinct text(6), precinct_group text(3), precinct_split text(6), precinct_suffix text(3),
    cong_dist text(3), house_dist text(3), senate_dist text(3), county_comm_dist text(3), school_brd_dist text(2), 
    first_instance date, last_instance date );
    ""","""
CREATE TABLE IF NOT EXISTS vl.voter_action_log (action_date date,
    action TEXT, result TEXT, info TEXT );
    ""","""
CREATE TABLE IF NOT EXISTS vl.voter_stats (action_date date, extract_file TEXT,  voters INTEGER, voter_status TEXT);
    """]

    for cmd in ddl_cmds:
        cur.execute(cmd)

    assert cur.fetchall() is not None, "failed to connect to monthly database"

def connect_dbs(cur, dbpath="data/db/"):
    setup = f"""
    ATTACH DATABASE '{dbpath}main_person.db' as vp; 
    ATTACH DATABASE '{dbpath}main_address.db' as va; 
    ATTACH DATABASE '{dbpath}main_districts.db' as vd;
    ATTACH DATABASE '{dbpath}main_log.db' as vl;

    """
    cur.executescript(setup)
    assert cur.fetchall() is not None, "failed to connect to main databases"

def connect_month(cur, dbname):
    cmd = f"""
    ATTACH DATABASE '{dbname}' as fl;
    """
    cur.execute(cmd)
    assert cur.fetchall() is not None, "failed to connect to monthly database"

def disconnect_month(cur):
    cmd = "DETACH DATABASE fl;"
    cur.execute(cmd)

    assert cur.fetchall() is not None, "failed to disconnect from monthly database"

def build_unique_indexes(cur):
    unique_rules = """
    CREATE UNIQUE INDEX IF NOT EXISTS vp.voter_person_unq__ind ON voter_person(voter_id, name_first, name_last, birth_date);
    CREATE UNIQUE INDEX IF NOT EXISTS va.voter_address_unq__ind ON voter_address(voter_id, address1, address2, city, state, zipcode);
    CREATE UNIQUE INDEX IF NOT EXISTS vd.voter_districts_unq__ind ON voter_districts(voter_id, precinct, precinct_group, precinct_split);

    CREATE INDEX IF NOT EXISTS vp.voter_person__voter_id__ind ON voter_person(voter_id);
    CREATE INDEX IF NOT EXISTS va.voter_address__voter_id__ind ON voter_address(voter_id);
    CREATE INDEX IF NOT EXISTS vd.voter_districts__voter_id__ind ON voter_districts(voter_id);

    """
    cur.executescript(unique_rules)

    assert cur.fetchall() is not None, "failed to build unique indexes for voters"

def build_newdata_tables(cur):
    unique_rules = """
    CREATE UNIQUE INDEX IF NOT EXISTS vp.voter_person_unq__ind ON voter_person(voter_id, name_first, name_last, birth_date);
    CREATE UNIQUE INDEX IF NOT EXISTS va.voter_address_unq__ind ON voter_address(voter_id, address1, address2, city, state, zipcode);
    CREATE UNIQUE INDEX IF NOT EXISTS vd.voter_districts_unq__ind ON voter_districts(voter_id, precinct, precinct_group, precinct_split);
    """
    cur.executescript(unique_rules)

    assert cur.fetchall() is not None, "failed to build newdata voter tables"

def populate_master(cur, srcfile):
    effective_date = srcfile[7:-3]
    logging.info(f"building knowledge from {srcfile}")

    # check for reloads
    cmd = f"SELECT action, info from vl.voter_action_log WHERE action = 'MERGE' and info = '{srcfile}'"
    res = cur.fetchall()
    if len(res) > 0 and len(res[0]) > 0 and res[0][1] == 'srcfile':
        logging.warning(f"{srcfile} has already been loaded, skipping it")
        # skip this file as it is already loaded
        return

    cmd = "PRAGMA SYNCHRONOUS=off;"
    cur.execute(cmd)

    build_unique_indexes(cur)

    main_dml = ["""
    INSERT INTO vp.voter_person (voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, first_instance, last_instance)
    SELECT voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, extract_date, extract_date
         -- fake where clause
         FROM fl.voter nv
         WHERE EXISTS (SELECT 1 FROM vp.voter_person ev WHERE ev.voter_id = nv.voter_id) 
         ON CONFLICT (voter_id, name_first, name_last, birth_date)
         DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
        ;
    ""","""
    INSERT INTO vp.voter_person (voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, first_instance, last_instance)
    SELECT voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, extract_date, extract_date
         -- fake where clause
         FROM fl.voter nv
         WHERE NOT EXISTS (SELECT 1 FROM vp.voter_person ev WHERE ev.voter_id = nv.voter_id) 
        ;
    ""","""
    INSERT INTO va.voter_address (voter_id, address_type, address1, address2, city, state, zipcode, first_instance, last_instance)
    SELECT voter_id, 'RES', res_address1, res_address2, res_city, res_state, res_zipcode, extract_date, extract_date
        FROM fl.voter nv
        WHERE EXISTS (SELECT 1 FROM va.voter_address ev WHERE ev.voter_id = nv.voter_id) 
        ON CONFLICT (voter_id, address1, address2, city, state, zipcode) 
            DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
        ;
    ""","""
    INSERT INTO va.voter_address (voter_id, address_type, address1, address2, city, state, zipcode, first_instance, last_instance)
    SELECT voter_id, 'RES', res_address1, res_address2, res_city, res_state, res_zipcode, extract_date, extract_date
        FROM fl.voter nv
        WHERE NOT EXISTS (SELECT 1 FROM va.voter_address ev WHERE ev.voter_id = nv.voter_id) 
        ;
    ""","""
    INSERT INTO va.voter_address (voter_id, address_type, address1, address2, city, state, zipcode, first_instance, last_instance)
    SELECT voter_id, 'RES', mail_address1, mail_address2, mail_city, mail_state, mail_zipcode, extract_date, extract_date
        FROM fl.voter nv
        WHERE EXISTS (SELECT 1 FROM va.voter_address ev WHERE ev.voter_id = nv.voter_id) 
        ON CONFLICT (voter_id, address1, address2, city, state, zipcode) 
            DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
        ;
    ""","""
    INSERT INTO va.voter_address (voter_id, address_type, address1, address2, city, state, zipcode, first_instance, last_instance)
    SELECT voter_id, 'MAIL', mail_address1, mail_address2, mail_city, mail_state, mail_zipcode, extract_date, extract_date
        FROM fl.voter nv
        WHERE NOT EXISTS (SELECT 1 FROM va.voter_address ev WHERE ev.voter_id = nv.voter_id) 
        ;
    ""","""
    INSERT INTO vd.voter_districts (voter_id, precinct, precinct_group, precinct_split, first_instance, last_instance)
    SELECT voter_id, precinct, precinct_group, precinct_split, extract_date, extract_date
        FROM fl.voter nv
        WHERE EXISTS (SELECT 1 FROM vd.voter_districts ev WHERE ev.voter_id = nv.voter_id)  
        ON CONFLICT (voter_id, precinct, precinct_group, precinct_split) 
            DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
        ;
    ""","""
    INSERT INTO vd.voter_districts (voter_id, precinct, precinct_group, precinct_split, first_instance, last_instance)
    SELECT voter_id, precinct, precinct_group, precinct_split, extract_date, extract_date
        FROM fl.voter nv
        WHERE NOT EXISTS (SELECT 1 FROM vd.voter_districts ev WHERE ev.voter_id = nv.voter_id)  
        ;
    """,f"""
    INSERT INTO vl.voter_action_log (action_date, action, info)
        VALUES (CURRENT_TIMESTAMP, 'MERGE', {srcfile});
    """,f"""
    INSERT INTO vl.voter_stats (action_date, extract_file, voters, voter_status)
        SELECT CURRENT_TIMESTAMP, '{srcfile}', count(*), voter_status from fl.voters;
    """]
    itr = 0
    for cmd in main_dml:
        st = time.time()
        try:
            cur.execute(cmd)
        except (sqlite3.OperationalError):
            logging.error(f"SQL error with {cmd}")
            raise sqlite3.OperationalError
        logging.info(f"statement {itr} took {(time.time() - st):.2f} seconds")
        itr += 1
    assert cur.fetchall() is not None, "failed to apply monthy file"

def populate_master_tuned(cur, srcfile):

    # check for reloads
    cmd = f"SELECT action, info from vl.voter_action_log WHERE action = 'MERGE' and info = '{srcfile}'"
    res = cur.fetchall()
    if len(res) > 0 and len(res[0]) > 0 and res[0][1] == 'srcfile':
        # skip this file as it is already loaded
        return

    cmd = "PRAGMA SYNCHRONOUS=off;"
    cur.execute(cmd)

    unique_rules = """
    CREATE UNIQUE INDEX IF NOT EXISTS vp.voter_person_unq__ind ON voter_person(voter_id, name_first, name_last, birth_date);
    CREATE UNIQUE INDEX IF NOT EXISTS va.voter_address_unq__ind ON voter_address(voter_id, address1, address2, city, state, zipcode);
    CREATE UNIQUE INDEX IF NOT EXISTS vd.voter_districts_unq__ind ON voter_districts(voter_id, precinct, precinct_group, precinct_split);

    """
    cur.executescript(unique_rules)

    main_dml = ["""
    INSERT INTO vp.voter_person (voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, first_instance, last_instance)
    SELECT voter_id, name_last, name_suffix, name_first, name_middle, gender, race, birth_date, registration_date,
    party_affiliation, day_phone_area_code, day_phone_number, day_phone_ext, voter_status, extract_date, extract_date
         -- fake where clause
         FROM fl.voter as f
         WHERE NOT EXISTS (SELECT 1 FROM voter_person as p
                WHERE p.voter_id = f.voter_id and
                  p.name_first = f.name_first and
                  p.name_last = f.name_last and
                  p.birth_date = f.birth_date)
         ON CONFLICT (voter_id, name_first, name_last, birth_date)
         DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
                    ;
    ""","""
    INSERT INTO va.voter_address (voter_id, address_type, address1, address2, city, state, zipcode, first_instance, last_instance)
    SELECT voter_id, address_type, address1, address2, city, state, zipcode, extract_date, extract_date)
        FROM fl.voter WHERE 1 = 1 
        ON CONFLICT (voter_id, address1, address2, city, state, zipcode, extract_date, extract_date) 
            DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
                    ;
    ""","""
    INSERT INTO va.voter_districts (voter_id, precinct, precinct_group, precinct_split, first_instance, last_instance)
    SELECT voter_id, precinct, precinct_group, precinct_split, extract_date, extract_date)
        FROM fl.voter WHERE 1 = 1 
        ON CONFLICT (voter_id, precinct, precinct_group, precinct_split) 
            DO UPDATE
                SET 
                    first_instance = 
                        CASE 
                            WHEN excluded.first_instance < first_instance 
                            THEN first_instance = excluded.first_instance
                            ELSE first_instance
                            END,
                    last_instance = 
                        CASE 
                            WHEN excluded.last_instance > last_instance 
                            THEN last_instance = excluded.last_instance
                            ELSE last_instance
                            END
                    ;

    """,f"""
    INSERT INTO vl.voter_action_log (action_date, action, info)
        VALUES (CURRENT_TIMESTAMP, 'MERGE', {srcfile});
    """,f"""
    INSERT INTO vl.voter_stats (action_date, extract_file, voters, voter_status)
        SELECT CURRENT_TIMESTAMP, '{srcfile}', count(*), voter_status from fl.voters;
    """]
    itr = 0
    for cmd in main_dml:
        st = time.time()
        try:
            cur.executescript(cmd)
        except (sqlite3.OperationalError):
            logging.error(f"SQL error with {cmd}")
            raise sqlite3.OperationalError
        logging.info(f"statement {itr} took {time.time() - st} seconds")
        itr += 1
    assert cur.fetchall() is not None, "failed to apply monthy file"

def mount_zipfile(zfilename, mntlocation):
    subprocess.call(["fuse-zip", "-r", zfilename, mntlocation])

def umount_zipfile(mntlocation):
    subprocess.call(["fusermount", "-u", mntlocation])


def main(args=[]):
    dbpath="data/db/"
    dbpath="tmpx/"
    db = sqlite3.connect(":memory:")
    cur = db.cursor()
    connect_dbs(cur, dbpath)
    master_tables_ddl(cur)
    db.commit()

    for monthlyfile in glob.glob(f"{dbpath}import_*.db"):
        connect_month(cur, monthlyfile)
        populate_master(cur, os.path.basename(monthlyfile))
        disconnect_month(cur)
        db.commit()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    main(sys.argv)