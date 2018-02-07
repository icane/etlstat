# etlstat

`etlstat` is a Python package that contains **Extract-Transform-Load**
utilities for statistical offices data processing.

It's been created by the Instituto Cántabro de Estadística (ICANE) and
belongs to ICANE's Python data framework.

## Testing
Data files are provided in order to allow execute and pass the unitary
tests in any development environment, including Travis
builds.

Database tests (mysql, oracle) require a local database instance
running into a Docker container.

### Oracle
Reference: [Using Oracle Database with Docker Engine] (https://www.toadworld.com/platforms/oracle/w/wiki/11638.using-oracle-database-with-docker-engine)

Pulling the Oracle Docker image
```sudo docker pull sath89/oracle-xe-11g```

Running the container
```sudo docker run --name orcldb -d -p 8080:8080 -p 1521:1521 sath89/oracle-xe-11g```

Connection parameters:
+ User: system
+ Password: oracle
+ Host name: localhost
+ Port: 1521
+ SID: xe

### MySQL
Reference: [Official repository mysql](https://hub.docker.com/_/mysql/)

Pulling the image
```sudo docker pull mysql```

Running the container
```sudo docker run --name mysqldb -d -p 3307:3306 -e MYSQL_ROOT_PASSWORD=password mysql[:<tag>]```

Notes:
  + in order to avoid conflicts with local MySQL instances and Travis builds, it is better to specify a non-standard MySQL port.
  + use IP 127.0.0.1 instead of _localhost_ to stablish a connection

Optional: **tag** is the tag specifying the MySQL version you want. See the list below for relevant tags.

+ 8.0.3, 8.0, 8
+ 5.7.21, 5.7, 5, latest
+ 5.6.39, 5.6
+ 5.5.59, 5.5

