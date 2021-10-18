

extract() {

echo "
PRAGMA SYNCHRONOUS=off;
PRAGMA cache_size=-2000000;
PRAGMA page_size=16386;

ATTACH DATABASE 'voter_import.db' AS voter;

CREATE TABLE main.voter AS
    SELECT * FROM voter.voter as v
        WHERE v.county_code = '$1';

CREATE INDEX IF NOT EXISTS voter__voter_id__ind on voter(voter_id);


CREATE TABLE main.voter_history AS
    SELECT * FROM voter.voter_history vh
        WHERE EXISTS (SELECT voter_id FROM main.voter v
          WHERE v.voter_id = vh.voter_id and
          v.county_code = '$1');

CREATE INDEX IF NOT EXISTS voter_history__voter_id__ind on voter_history(voter_id, election_date);
" | sqlite3 "$2"
}


extract $1 voter_import_${1}.sqlite

