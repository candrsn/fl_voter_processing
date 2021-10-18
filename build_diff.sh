
export TMPDIR=~/media/big_working

sqlite3 fl_voters_20190813.gpkg <<EOF

PRAGMA synchronous=off;

CREATE UNIQUE INDEX IF NOT EXISTS voter__voter_id__ind on voter(voter_id);

ATTACH DATABASE 'fl_voters_20190709.gpkg' as prev;
CREATE UNIQUE INDEX IF NOT EXISTS prev.voter__voter_id__ind on voter(voter_id);


--SELECT v.voter_id, v.county_code, v.name_last, pv.name_last, pv.county_code FROM
--    voter v LEFT OUTER JOIN prev.voter as pv ON (v.voter_id = pv.voter_id)
--    WHERE v.county_code <> pv.county_code or pv.county_code is NULL;


DROP TABLE IF EXISTS voter_address;
CREATE TABLE voter_address as
    SELECT count(*) as instances, 
        res_address1 as address1, res_address2 as address2, CAST('' as TEXT) as address3, 
	res_city as city, res_state as state, res_zipcode as zipcode, 'USA' as country, 
        min(voter_id) as minvoterid, max(voter_id) as maxvoterid
      FROM voter
      WHERE (length(res_address1) + length(res_address2)) > 0 
      GROUP BY 2,3,4,5,6,7,8
    UNION
    SELECT count(*) as instances,
        mail_address1 as address1, mail_address2 as address2, mail_address3 as address3, 
        mail_city as city, mail_state as state, mail_zipcode as zipcode, mail_country as country,
        min(voter_id) as minvoterid, max(voter_id) as maxvoterid
      FROM voter 
      WHERE (length(mail_address1) + length(mail_address2) + length(mail_address3)) > 0 
      GROUP BY 2,3,4,5,6,7,8;


UPDATE voter_address SET 
    address1 = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(address1)),'  ',' '+ CHAR(7)) , CHAR(7)+' ',''), CHAR(7),''),
    address2 = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(address2)),'  ',' '+ CHAR(7)) , CHAR(7)+' ',''), CHAR(7),''),
    address3 = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(address3)),'  ',' '+ CHAR(7)) , CHAR(7)+' ',''), CHAR(7),''),
    city = REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(city)),'  ',' '+ CHAR(7)) , CHAR(7)+' ',''), CHAR(7),'')
;

DROP TABLE IF EXISTS voter_zipcode;
CREATE TABLE voter_zipcode as
    SELECT sum(instances) as instances, city, state, 
      CASE
	WHEN country in ('USA', 'UNITED STATES') THEN substr(zipcode,1,5)
        ELSE zipcode END as zipcode, country,
      min(minvoterid) as minvoterid, max(maxvoterid) as maxvoterid
      FROM voter_address
      GROUP BY 2,3,4,5;
SELECT * FROM voter_zipcode WHERE zipcode > '' and instances > 4
    ORDER BY zipcode LIMIT 50 OFFSET 200;

SELECT instances, address1, address2, address3, city, state, zipcode, country
    FROM voter_address
    -- WHERE length(Address1) > 0
    WHERE instances > 2
    ORDER BY city, zipcode, address1
    LIMIT 50
    OFFSET 5000
   ;

EOF


