#!/bin/bash
dropdb test_db
createdb test_db
psql test_db < $HOME/postgis/postgis/postgis.sql > /dev/null
psql test_db < $HOME/postgis/spatial_ref_sys.sql > /dev/null
psql test_db << END_SQL
CREATE TABLE test_pg (id SERIAL PRIMARY KEY, name TEXT NOT NULL);
SELECT AddGeometryColumn('test_pg', 'geometry', 3946, 'POLYGON', 3);
END_SQL

#x=1829995
#xp=1869005
#y=5150995
#yp=5195005
#echo "INSERT INTO test_pg (name, geometry) VALUES ('toto', ST_GeometryFromText('POLYGON(($x $y 0,$xp $y 0,$xp $yp 0,$x $yp 0,$x $y 0 ))', 3946));" | psql test_db

rm /tmp/tmp.sql

for x in {1829995..1869005..100}; 
do 
    for y in {5150995..5195005..100}; 
    do 
        xp=$(($x + 50))
        yp=$(($y + 50))
        echo  "INSERT INTO test_pg (name, geometry) VALUES ('toto', ST_GeometryFromText('POLYGON(($x $y 500,$xp $y 500,$xp $yp 500,$x $yp 500,$x $y 500 ))', 3946));" >> /tmp/tmp.sql
    done
done

psql test_db < /tmp/tmp.sql

