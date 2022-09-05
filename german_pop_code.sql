CREATE TABLE census2014 (
    county_id smallint PRIMARY KEY,
    county varchar(40),
    aggregat varchar(20),
    population2014 int);


COPY census2014
FROM 'C:\__Local Disk D\SQL\census2014.csv'
WITH (FORMAT CSV, header);


CREATE TABLE census2019 (
    county_id smallint PRIMARY KEY,
    county varchar(40),
    aggregat varchar(20),
    population2019 int);


COPY census2019
FROM 'C:\__Local Disk D\SQL\census2019.csv'
WITH (FORMAT CSV, header);


ALTER TABLE census2019 ADD CONSTRAINT pop_check CHECK (population2019 > 0);
ALTER TABLE census2019 ADD CONSTRAINT county_unq UNIQUE (county);
ALTER TABLE census2019 DROP CONSTRAINT county_unq;


EXPLAIN ANALYZE SELECT * 
FROM census2019
WHERE county LIKE '%burg';


CREATE INDEX county_indx ON census2019 (county);


EXPLAIN ANALYZE SELECT * 
FROM census2019
WHERE county LIKE '%burg';


DROP INDEX county_indx;


SELECT c19.county_id,
        c19.county,
        c19.aggregat,
        c19.population2019 AS pop19,
        c14.population2014 AS pop14,
        c19.population2019 - c14.population2014 AS raw_change,
        round( (c19.population2019::numeric - c14.population2014) / c14.population2014 * 100, 1) AS pct_change
FROM census2019 AS c19 JOIN census2014 AS c14
USING (county_id)
WHERE round( (c19.population2019::numeric - c14.population2014) / c14.population2014 * 100, 1) > 5 OR 
    round( (c19.population2019::numeric - c14.population2014) / c14.population2014 * 100, 1) < -4 
ORDER BY pct_change DESC;


CREATE TEMPORARY TABLE census14_19
AS (SELECT c19.county_id,
        c19.county,
        c19.aggregat,
        c14.population2014 AS pop14,
        c19.population2019 AS pop19
FROM census2019 AS c19 JOIN census2014 AS c14
USING (county_id)
ORDER BY c19.county_id
);


SELECT aggregat,
       SUM (pop19-pop14) AS raw_change
FROM census14_19
GROUP BY aggregat;


SELECT *,
    pop19 - pop14 AS trend
FROM census14_19
WHERE county_id between 3000 and 4000;


ALTER TABLE census14_19 ADD COLUMN bundesland text;


UPDATE census14_19
SET bundesland = CASE
    WHEN county_id >= 16000 THEN 'Thuringia'    
    WHEN county_id >= 15000 THEN 'Saxony-Anhalt'
    WHEN county_id >= 14000 THEN 'Saxony'
    WHEN county_id >= 13000 THEN 'Mecklenburg Western Pomerania'
    WHEN county_id >= 12000 THEN 'Brandenburg'
    WHEN county_id >= 11000 THEN 'Berlin'
    WHEN county_id >= 10000 THEN 'Saarland'    
    WHEN county_id >= 9000 THEN 'Bavaria'
    WHEN county_id >= 8000 THEN 'Baden-Wuertemberg'
    WHEN county_id >= 7000 THEN 'Rhineland Palatinate'
    WHEN county_id >= 6000 THEN 'Hesse'
    WHEN county_id >= 5000 THEN 'North Rhine-Westphalia (NRW)'
    WHEN county_id >= 4000 THEN 'Bremen'
    WHEN county_id >= 3000 THEN 'Lower Saxony'
    WHEN county_id >= 2000 THEN 'Hamburg'
    WHEN county_id >= 1000 THEN 'Schleswig-Holstein'
    END


SELECT *
FROM census14_19
ORDER BY county_id;


SELECT bundesland,
    count(*) AS total_counties
FROM census14_19
GROUP BY bundesland
ORDER BY count(*) DESC;


SELECT bundesland,
        county,
        pop19,
RANK () OVER (PARTITION  BY bundesland
ORDER BY pop19 DESC)
FROM census14_19;


SELECT bundesland,
        SUM (pop14) AS pop14,
        SUM (pop19) AS pop19,
        SUM (pop19-pop14) AS raw_change
FROM census14_19
GROUP BY bundesland
ORDER BY raw_change DESC;


SELECT *
FROM census14_19
WHERE county ILIKE 'old%';


START TRANSACTION;

UPDATE census14_19
SET county = 'OLDENBURG STADT'
WHERE county ILIKE 'oldenBURG %';

SELECT *
FROM census14_19
WHERE county ILIKE 'old%';

ROLLBACK;


ALTER TABLE census14_19
RENAME TO final_pop_data;


COPY final_pop_data (county, pop14, pop19, bundesland)
TO 'C:\__Local Disk D\SQL\final_pop_data.csv'
WITH (FORMAT CSV, header);