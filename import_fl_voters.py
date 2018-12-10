
import sys
import os
import json
import unicodecsv
import glob
import sqlite3
import zipfile

class DB():
    def __init__(self, dbFile, newDB=False):
        if newDB and os.path.exists(dbFile):
            os.remove(dbFile)
        self.conn = sqlite3.connect(dbFile)
    
    def __delete__(self):
        self.conn.commit()
        self.conn.close()

    def connect(self):
        return self.conn

    def cursor(self):
        c = self.conn.cursor()
        c.execute('''PRAGMA SYNCHRONOUS=off''')
        return c

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()

voter_def = {
    "table": 'voter',
    "fields": [
    ['county_code', 'text(3)', None, 3],
    ['voter_id', 'text(10)', None, 10],
    ['name_last', 'text(30)', 'suppressed', 30],
    ['name_suffix', 'text(5)', 'suppressed', 5], 
    ['name_first', 'text(30)', 'suppressed', 30],
    ['name_middle', 'text(30)', 'suppressed', 30],
    ['is_suppressed', 'text(1)', 'suppressed', 1],
    ['res_address1', 'text(50)', 'suppressed', 50],
    ['res_address2', 'text(40)', 'suppressed', 40],
    ['res_city', 'text(40)', 'suppressed', 40],
    ['res_state', 'text(2)', 'suppressed', 2],
    ['res_zipcode', 'text(10)', 'suppressed', 10],
    ['mail_address1', 'text(40)', 'suppressed', 40],
    ['mail_address2', 'text(40)', 'suppressed', 40],
    ['mail_address3', 'text(40)', 'suppressed', 40],
    ['mail_city', 'text(40)', 'suppressed', 40],
    ['mail_state', 'text(2)', 'suppressed', 2],
    ['mail_zipcode', 'text(12)', 'suppressed', 12],
    ['mail_country', 'text(40)', 'suppressed', 40],
    ['gender', 'text(1)', None, 1],
    ['race', 'text(1)', None, 1],
    ['birth_date', 'text(10)', 'suppressed', 10],
    ['registration_date', 'text(10)', None, 10],
    ['party_affiliation', 'text(3)', None, 3],
    ['precinct', 'text(6)', None, 6],
    ['precinct_group', 'text(3)', None, 3],
    ['precinct_split', 'text(6)', None, 6],
    ['precinct_suffix', 'text(3)', None, 3],
    ['voter_status', 'text(3)', None, 3],
    ['cong_dist', 'text(3)', None, 3],
    ['house_dist', 'text(3)', None, 3],
    ['senate_dist', 'text(3)', None, 3],
    ['county_comm_dist', 'text(3)', None, 3],
    ['school_brd_dist', 'text(2)', None, 2],
    ['day_phone_area_code', 'text(3)', 'suppressed', 3],
    ['day_phone_number', 'text(7)', 'suppressed', 7],
    ['day_phone_ext', 'text(4)', 'suppressed', 4],
    ['email', 'text(100)', 'suppressed', 100],
    ['extract_date', 'text(10)', 'unknown', 10]],
    "zipGlob": '*Registration*.zip', 
    "fileGlob": 'voter/???_2*.txt'
    }

voter_history_def = {
    "table": 'voter_history', 
    "fields": [
    ['county_code', 'text(3)', None, 3],
    ['voter_id', 'text(10)', None, 10],
    ['election_date', 'text(10)', None, 10],
    ['election_type', 'text(10)', None, 10],
    ['history_code', 'text(1)', None, 1]], 
    "zipGlob": '*History*.zip', 
    "fileGlob": 'history/???_H_2*txt'
    }

history_codes_def = {
    "table": 'history_code_lu', 
    "fields": [['history_code', 'text(1)', None],
     ['definition', 'text', None]],
    "data": [
    ['A', 'Voted Absentee'],
    ['B', 'Absentee Ballot - Not Counted'],
    ['E', 'Voted Early'],
    ['N', 'Did Not Vote'],
    ['P', 'Provisional Ballot - Not Counted'],
    ['Y', 'Voted at Polls']]
    }

party_affiliation_def = {
    "table": 'party_affiliation_lu', 
    "fields": [['party_code', 'text(3)', None],
     ['definition', 'text', None] ], 
    "data": [
    ['AIP', 'American\'s Party of Florida'],
    ['CFP', 'Constitution Party of Florida'],
    ['DEM', 'Florida Democratic Party'],
    ['ECO', 'Ecology Party of Florida'],
    ['GRE', 'Green Party of Florida'],
    ['IND', 'Independent Party of Florida'],
    ['LPF', 'Libertarian Party of Florida'],
    ['NPA', 'No Party Affiliation'],
    ['PSL', 'Party for Socialism and Liberation - Florida'],
    ['REF', 'Reform Party of Florida'],
    ['REP', 'Republican Party of Florida']
    ]}

race_def = {
    "table": 'race_code_lu', 
    "fields": [['race_code', 'text(3)', None],
     ['definition', 'text', None]
    ],
    "data": [
    ['1', 'American Indian or Alaska Native'],
    ['2', 'Asian or Pacific Islander'],
    ['3', 'Black, Not Hispanic'],
    ['4', 'Hispanic'],
    ['5', 'White, Not Hispanic'],
    ['6', 'Other'],
    ['7', 'Multi-racial'],
    ['9', 'unknown']]
    }
    
gender_def = {
    "table": 'gender_code_lu', 
    "fields": [['gender_code', 'text(1)', None],
     ['definition', 'text', None]], 
    "data": [
    ['F', 'Female'],
    ['M', 'Male'],
    ['U', 'Unknown']
    ]}

voter_status_def = {
    "table": 'voter_status_lu',  
    "fields": [['status_code', 'text(3)', None],
     ['definition', 'text', None]],
    "data": [
    ['ACT', 'Active'],
    ['INA', 'Inactive']
    ]
    }

county_code_def = {
    "table": 'county_code_lu', 
    "fields": [['county_code', 'text(3)', None],
     ['definition', 'text', None]],
    "data": [
    ["ALA","Alachua"],
    ["GUL","Gulf"],
    ["NAS","Nassau"],
    ["BAK","Baker"],
    ["HAM","Hamilton"],
    ["OKA","Okaloosa"],
    ["BAY","Bay"],
    ["HAR","Hardee"],
    ["OKE","Okeechobee"],
    ["BRA","Bradford"],
    ["HEN","Hendry"],
    ["ORA","Orange"],
    ["BRE","Brevard"],
    ["HER","Hernando"],
    ["OSC","Osceola"],
    ["BRO","Broward"],
    ["HIG","Highlands"],
    ["PAL","Palm Beach"],
    ["CAL","Calhoun"],
    ["HIL","Hillsborough"],
    ["PAS","Pasco"],
    ["CHA","Charlotte"],
    ["HOL","Holmes"],
    ["PIN","Pinellas"],
    ["CIT","Citrus"],
    ["IND","Indian River"],
    ["POL","Polk"],
    ["CLA","Clay"],
    ["JAC","Jackson"],
    ["PUT","Putnam"],
    ["CLL","Collier"],
    ["JEF","Jefferson"],
    ["SAN","Santa Rosa"],
    ["CLM","Columbia"],
    ["LAF","Lafayette"],
    ["SAR","Sarasota"],
    ["DAD","Miami-Dade"],
    ["LAK","Lake"],
    ["SEM","Seminole"],
    ["DES","Desoto"],
    ["LEE","Lee"],
    ["STJ","St. Johns"],
    ["DIX","Dixie"],
    ["LEO","Leon"],
    ["STL","St. Lucie"],
    ["DUV","Duval"],
    ["LEV","Levy"],
    ["SUM","Sumter"],
    ["ESC","Escambia"],
    ["LIB","Liberty"],
    ["SUW","Suwannee"],
    ["FLA","Flagler"],
    ["MAD","Madison"],
    ["TAY","Taylor"],
    ["FRA","Franklin"],
    ["MAN","Manatee"],
    ["UNI","Union"],
    ["GAD","Gadsden"],
    ["MRN","Marion"],
    ["VOL","Volusia"],
    ["GIL","Gilchrist"],
    ["MRT","Martin"],
    ["WAK","Wakulla"],
    ["GLA","Glades"],
    ["MON","Monroe"],
    ["WAL","Walton"],
    ["WAS","Washington"]]
    }

lu_tables = [county_code_def, voter_status_def, gender_def, race_def, party_affiliation_def, history_codes_def]
data_tables = [voter_def, voter_history_def]

def build_table(db, data):
    cmd = "CREATE TABLE IF NOT EXISTS {tbl} (".format(tbl=data["table"])
    sep = ""
    for fld in data["fields"]:
        cmd += sep + fld[0] + " " + fld[1]
        sep = ", "
    cmd += ");"

    cur = db.cursor()
    cur.execute(cmd)
    cur.fetchone()
    db.commit()

def init_gpkg(db):
    cmd = "SELECT load_extension('mod_spatialite.so');"
    cur = db.cursor()
    cur.execute(cmd)
    cur.fetchone()

    cmd = "SELECT gpkgCreateBaseTables();"
    cur.execute(cmd)
    cur.fetchone()
    db.commit()

def load_table(db, data):
    cmd = "INSERT INTO {tbl} VALUES (".format(tbl=data["table"])
    sep = ""
    for fld in data["fields"]:
        cmd += sep + '?'
        sep = ", "
    cmd += ");"

    cur = db.cursor()
    cur.executemany(cmd, data["data"])
    cur.fetchone()
    db.commit()

def parse_dataline(data,  line):
    row = []
    if 1 == 1 :
        row = line.decode('utf-8').split("\t")
        for p in range(0, len(data["fields"])-len(row)):
            row.append("")
    else:
        ptr = 0
        for fld in data["fields"]:
            sz= int(fld[3])
            r = line[ptr:sz]
            ptr += sz
            row.append(r)
    return row

def load_data_table(db, data, dvdLbl):
    cur = db.cursor()

    # read directly from the ZIP File
    for z in glob.glob(('dvd/%s/'+ data['zipGlob'])%(dvdLbl)):
        zip_ref = zipfile.ZipFile(z, 'r')
        
        #for f in glob.glob(data["fileGlob"]):
        # only grab TXT files from the archive
        zF = [z.filename for z in zip_ref.infolist() if z.filename[-4:] == '.txt']
        for f in zF:
            fp = zip_ref.open(f, 'r')
            ck = []
            for l in fp:
                # this step adds missing fields and converts from bytes to string
                lp = parse_dataline(data, l)
                ck.append (lp)
                if len(ck) >= 100000:
                    load_data_chunk(cur, data, ck)
                    print(".", end='')
                    ck = []
            if len(ck) > 0:
                load_data_chunk(cur, data, ck)
                print(".", end='')
            fp.close()
            print(': ' + f)

def load_data_chunk(cur, data, chunk):
    cmd = "INSERT INTO {tbl} VALUES (".format(tbl=data["table"])
    sep = ""
    for fld in data["fields"]:
        cmd += sep + '?'
        sep = ", "
    cmd += ");"

    cur.executemany(cmd, chunk)
    cur.fetchone()

def main(args):
    dbF = 'fl_voters.gpkg'
    if os.path.exists(dbF):
        os.remove(dbF)

    if len(args[0]) > 0:
        dvdLbl = args[0]
    else:
        dvdLbl = 'Jul_10_2018'

    db = DB(dbF)
    for d in lu_tables:
        build_table(db, d)
        load_table(db, d)
    db.commit()

    for d in data_tables:
        build_table(db, d)
        load_data_table(db, d, dvdLbl)
        db.commit()

    db.close()

if "__main__" == __name__:
    assert sys.version.startswith('3.'), "Must run with python 3.6 or later"

    main(sys.argv[1:])
    print ("All Done")
