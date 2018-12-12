LOAD DATA
CHARACTERSET UTF8
INFILE '/home/lla11358/data/git/python/etlstat/etlstat/database/test/px_01001.dat'
APPEND
INTO TABLE test.px_01001
FIELDS TERMINATED BY ';' OPTIONALLY ENCLOSED BY '"'
TRAILING NULLCOLS
(id,tipo_indicador,nivel_educativo,valor)