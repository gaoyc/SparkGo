--CREATE TABLE IF NOT EXISTS TB_LTESMS
--(
--BEGINTIME         bigint,
--USERNUM         string,
--HOMEAREA         string,
--RELATENUM         string,
--RELATEHOMEAC         string,
--IMSI         string,
--IMEI         string,
--CURAREA         string,
--NEID         string,
--LAI         int,
--CI         string,
--LONGITUDE         float,
--LATITUDE         float,
--CALLTYPE         string,
--SENDTIME         bigint,
--MESSAGE         string,
--SID         string,
--MSGTAG         string,
--IDFLAG         int,
--RAWRELATENUM         string,
--TMSI         string,
--SPCODE         int,
--GUTI         string,
--CGI         string,
--TAGS         string,
--EXTRA_LONGITUDE         float,
--EXTRA_LATITUDE         float,
--GEOHASH         string,
--UUID         string,
--NEIDIPV6         STRING,
--AREACODE         INT
--)
--partitioned by (cp bigint, ld bigint)
--stored as orc tblproperties ("orc.compress"="ZLIB");

DROP TABLE IF EXISTS TB_LTESMS_EXT;
CREATE EXTERNAL TABLE IF NOT EXISTS TB_LTESMS_EXT
(BEGINTIME         bigint,
USERNUM         string,
HOMEAREA         string,
RELATENUM         string,
RELATEHOMEAC         string,
IMSI         string,
IMEI         string,
CURAREA         string,
NEID         string,
LAI         int,
CI         string,
LONGITUDE         float,
LATITUDE         float,
CALLTYPE         string,
SENDTIME         bigint,
MESSAGE         string,
SID         string,
MSGTAG         string,
IDFLAG         int,
RAWRELATENUM         string,
TMSI         string,
SPCODE         int,
GUTI         string,
CGI         string,
TAGS         string,
EXTRA_LONGITUDE         float,
EXTRA_LATITUDE         float,
GEOHASH         string,
UUID         string,
NEIDIPV6         STRING,
AREACODE         INT
)
partitioned by (cp bigint, ld bigint)
stored as orc
location 'file:///mnt/workIng/testdata/warehousre/tb_ltesms_ext';
