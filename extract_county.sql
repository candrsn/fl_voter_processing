
PRAGMA SYNCHRONOUS=off;
PRAGMA cache_size=-2000000;
PRAGMA page_size=16386;

DELETE FROM voter WHERE not county_code = 'MAN';
VACUUM;

CREATE INDEX IF NOT EXISTS voter__voter_id__ind on voter(voter_id);
DELETE FROM voter_history
  WHERE not EXISTS (SELECT voter_id FROM voter v
      WHERE v.voter_id = voter_history.voter_id and
        v.county_code = 'MAN');

VACUUM;
