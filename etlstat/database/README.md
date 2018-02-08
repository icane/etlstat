# Installation of modules required to get access to relational databases.

## MySQL Connector
Installation of MySQL connector for Python 3.
If the version is not specified
If the version is not specified, the default attempts to install 2.2.3
and fails: [Unable to find Protobuf include directory](http://stackoverflow.com/questions/43029672/unable-to-find-protobuf-include-directory)

```
> sudo pip3 install mysql-connector==2.1.4
Collecting mysql-connector==2.1.4
  Downloading mysql-connector-2.1.4.zip (355kB)
    100% |████████████████████████████████| 358kB 1.7MB/s 
Installing collected packages: mysql-connector
  Running setup.py install for mysql-connector ... done
Successfully installed mysql-connector-2.1.4
```

Download and install the last version:
```
    https://dev.mysql.com/downloads/connector/python/

    > sudo dpkg -i <connector_file_name.deb>
```

## cx_Oracle
Cx_Oracle requires Oracle SQLNet. Read [Connecting to Oracle11g databases from Python scripts](/var/git/md/docs/dev/python/oracle_connection.md).
```
# apt-get install python3-dev

# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:"/opt/oracle/instantclient_11_2"

# export PATH=$PATH:"/opt/oracle/instantclient_11_2"

# pip3 install cx_oracle
Collecting cx_oracle
  Using cached cx_Oracle-5.3.tar.gz
Installing collected packages: cx-oracle
  Running setup.py install for cx-oracle ... done
Successfully installed cx-oracle-5.3
```
## Mysql-python

```
> sudo apt-get install python-dev libmysqlclient-dev


```

## Testing
Database tests require a local database instance running into a Docker container.

### Oracle
Reference: [Using Oracle Database with Docker Engine](https://www.toadworld.com/platforms/oracle/w/wiki/11638.using-oracle-database-with-docker-engine)

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
