# Instalación módulos de acceso a bases de datos relacionales (Python3)

## MySQL Connector
Instalación del conector MySQL para Python 3. Si no se especifica la versión, se intenta instalar la 2.2.3 y se produce un error: [Unable to find Protobuf include directory](http://stackoverflow.com/questions/43029672/unable-to-find-protobuf-include-directory)  

```
> sudo pip3 install mysql-connector==2.1.4
Collecting mysql-connector==2.1.4
  Downloading mysql-connector-2.1.4.zip (355kB)
    100% |████████████████████████████████| 358kB 1.7MB/s 
Installing collected packages: mysql-connector
  Running setup.py install for mysql-connector ... done
Successfully installed mysql-connector-2.1.4
```

Descarga e instalación de la última versión en .deb:
```
    https://dev.mysql.com/downloads/connector/python/

    > sudo dpkg -i nombre_de_archivo
```

## cx_Oracle
Es necesario tener instalado Oracle SQLNet. Ver el documento [Connecting to Oracle11g databases from Python scripts](/var/git/md/docs/dev/python/oracle_connection.md). 
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

# Testing MySql class

Es necesario actualizar el mapa de configuración al principio del archivo testMySql.py:
```
    MYSQL = {
        'SERVER'    : 'localhost',
        'DATABASE'  : 'test',
        'PORT'      : '',
        'USER'      : '',
        'PASSWORD'  : ''
    }
```
(Se recomienda el uso de la base de datos local para la prueba, puerto por defecto 3306)

# Uso de los módulos database

## Referencias
[Connecting to MySQL databases from Python scripts](/var/git/md/docs/dev/python/mysql_connection.md)
[Connecting to Oracle11g databases from Python scripts](/var/git/md/docs/dev/python/oracle_connection.md)