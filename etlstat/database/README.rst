================
etlstat modules
================

database module
----------------

Provides methods to process sets of data files in PC-Axis, CSV, positional TXT,
XML or XLS format.

testing
-------
Data files are provided in order to allow execute and pass the unit tests in
any development environment, including Travis builds.


Installation of modules required to get access to relational databases
----------------------------------------------------------------------

MySQL Connector
...............

Installation of MySQL connector for Python 3.
If the version is not specified, pip attempts to install 2.2.3 and fails: [Unable to find Protobuf include directory](http://stackoverflow.com/questions/43029672/unable-to-find-protobuf-include-directory)

:code:`sudo pip3 install mysql-connector==2.1.4`

Download and install the last version from https://dev.mysql.com/downloads/connector/python/:

:code:`sudo dpkg -i <connector_file_name.deb>`

cx_Oracle
.........

Cx_Oracle requires [Oracle Instant Client](https://www.oracle.com/technetwork/database/database-technologies/instant-client/overview/index.html).

Download and install Oracle Instant Client
++++++++++++++++++++++++++++++++++++++++++

1. Download version 12.2.0.1.0 for Linux_x86_64 [Instant Client Downloads for Linux x86-64 (64-bit)](http://www.oracle.com/technetwork/topics/linuxx86-64soft-092277.html).
   1. Instant Client Package - Basic: All files required to run OCI, OCCI, and JDBC-OCI applications [instantclient-basic-linux.x64-12.2.0.1.0.zip](http://download.oracle.com/otn/linux/instantclient/122010/instantclient-basic-linux.x64-12.2.0.1.0.zip)
   2. Instant Client Package - SQL*Plus: Additional libraries and executable for running SQL*Plus with Instant Client [instantclient-sqlplus-linux.x64-12.2.0.1.0.zip](http://download.oracle.com/otn/linux/instantclient/122010/instantclient-sqlplus-linux.x64-12.2.0.1.0.zip)
   3. Instant Client Package - SDK: Additional header files and an example makefile for developing Oracle applications with Instant Client [instantclient-sdk-linux.x64-12.2.0.1.0.zip](http://download.oracle.com/otn/linux/instantclient/122010/instantclient-sdk-linux.x64-12.2.0.1.0.zip)
   4. Instant Client Package - Tools: Includes Data Pump, SQL*Loader and Workload Replay Client [instantclient-tools-linux.x64-12.2.0.1.0.zip](http://download.oracle.com/otn/linux/instantclient/122010/instantclient-tools-linux.x64-12.2.0.1.0.zip)
2. Unzip the packages (as root) into a single directory such as
    "/opt/oracle/instantclient_11_2" that is accessible to your application.
3. Create the appropriate libclntsh.so and libocci.so links for the version
    of Instant Client.
4. Set the environment variable LD_LIBRARY_PATH pointing to the directory
    created in Step 2.
5. To use supplied binaries such as SQL*Plus, update your PATH environment
    variable.

::

    sh
    export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"/opt/oracle/instantclient_18_3"
    export PATH=$PATH:"/opt/oracle/instantclient_18_3"


+ Test your Instant Client install by using "sqlplus" or "sqlplus64" to
    connect to the database.
+ Edit **/etc/ld.so.conf.d/oracle.conf**. This is a new file, simply add
    the location of .so files here, then update the ldpath executing
    **ldconfig**.

Install cx_Oracle Python package
++++++++++++++++++++++++++++++++

Before install cx_oracle with pip, be sure LD_LIBRARY_PATH and PATH
environment variables points to oracle installation directory. ::

   sh
   pip3 install python3-dev
   pip3 install cx_oracle


Mysql-python
............

:code:`sudo apt-get install python-dev libmysqlclient-dev`

Testing
-------

Database tests require local database instances. The following explains
how to run such instances in Docker containers.

Oracle
......

References:
    + [Docker Images from Oracle](https://github.com/oracle/docker-images)
    + [Running Oracle 11g Release 2 (11.2.0.2) XE in Docker Container](http://ajitabhpandey.info/2018/02/running-oracle-11g-release-2-11-2-0-2-xe-in-docker-container/)

Build the Oracle Docker image for Oracle 11g Express Edition

1. Clone the Oracle Docker images repository (https://github.com/oracle/docker-images)
2. Download Oracle Database 11g Release 2 Express Edition for Linux x64 from
    http://download.oracle.com/otn/linux/oracle11g/xe/oracle-xe-11.2.0-1.0.x86_64.rpm.zip
3. Build the image as specified in
    OracleDatabase/SingleInstance/dockerfiles/11.2.0.2/Dockerfile.xe
    (renaming Dockerfile.xe to Dockerfile)
4. Runinng the container
    :code::
        sudo docker run --name oracle --shm-size=1g -p 1521:1521 -p 8080:8080 \
        -e ORACLE_PWD=oracle -v /var/opt/devdb/oradata:/u01/app/oracle/oradata \
        oracle/database:11.2.0.2-xe

5. Connection parameters
    5.1. User: system
    5.2. Password: oracle
    5.3. Host name: localhost
    5.4. Port: 1521
    5.5. SID: xe

MySQL
.....

Reference: [Official repository mysql](https://hub.docker.com/_/mysql/)

Pulling the image
:code:`sudo docker pull mysql`

Running the container

:code::
    sudo docker run --name mysqldb -d -p 3306:3306 \
    -e MYSQL_ROOT_PASSWORD=password mysql[:<tag>]

Notes:

+ use IP 127.0.0.1 instead of _localhost_ to stablish a connection

Optional: **tag** is the tag specifying the MySQL version you want. See
the list below for relevant tags.

+ 8.0.3, 8.0, 8
+ 5.7.21, 5.7, 5, latest
+ 5.6.39, 5.6
+ 5.5.59, 5.5
