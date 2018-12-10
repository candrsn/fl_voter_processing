
PRAGMA SYNCHRONOUS=OFF;

.mode csv
.headers on
.output voter_address2.csv

SELECT DISTINCT  res_address, res_address1, res_city, res_state, res_zipcode
  FROM (SELECT 
    CASE
      WHEN res_address2 > ' ' THEN
        res_address1 || ', ' || res_address2
      ELSE
        res_address1
      END as res_address,
      res_address1,
      res_city, CASE WHEN res_state > ' ' THEN res_state ELSE 'FL' END as res_state, res_zipcode
	FROM (
	SELECT CASE WHEN substr(res_address1,1,2) = '0 ' THEN substr(res_address1,3,80)
	       ELSE res_address1 END as res_address1, res_address2, res_city, res_state, res_zipcode
	    FROM voter limit 500000000000000)  as g
	    ) as t
  ORDER BY 5,3,1; 

.output voter_address.csv
SELECT DISTINCT res_address, res_address1, res_city, res_state, res_zipcode
  FROM (SELECT 
    CASE
      WHEN res_address2 > ' ' THEN
        res_address1 || ', ' || res_address2
      ELSE
        res_address1
      END as res_address,
      res_address1,
      res_city, CASE WHEN res_state > ' ' THEN res_state ELSE 'FL' END as res_state, res_zipcode
	FROM (
	SELECT res_address1, res_address2, res_city, res_state, res_zipcode
	    FROM voter limit 500000000000000)  as g
	    ) as t
  ORDER BY 5,3,1; 
