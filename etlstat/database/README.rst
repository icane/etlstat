================
etlstat modules
================

database module
----------------

Provides methods to process sets of data files in PC-Axis, CSV, positional TXT,
XML or XLS format.

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


PostgreSQL
..........

Psycopg2 is the most popular python driver, required for most Python+Postgres frameworks. ::

    sh
    pip3 install psycopg2


Testing
-------

Database tests require local database instances. The following explains
how to run such instances in Docker containers.

Oracle
......

:code:`docker run -d -p 1521:1521 oracleinanutshell/oracle-xe-11g`

Connection parameters: ::

    user = 'system' or 'sys'
    password = 'oracle'
    host = 'localhost'
    port = '1521'
    service_name = 'xe'

MySQL
.....

:code:`docker run -d -p 3306:3306 -e MYSQL_ROOT_PASSWORD=password mysql:5.7`

Connection parameters: ::

    user = 'root'
    password = 'password'
    host = '127.0.0.1'
    port = '3306'
    database = ''


Optional: **tag** is the tag specifying the MySQL version you want. See the list below for relevant tags. ::

+ 8.0.3, 8.0, 8
+ 5.7.21, 5.7, 5, latest
+ 5.6.39, 5.6
+ 5.5.59, 5.5

PostgreSQL
..........

:code:`docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=password postgres`

Connection parameters: ::

    user = 'postgres'
    password = 'password'
    host = 'localhost'
    port = '5432'
    database = 'postgres'
