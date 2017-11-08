"""

"""
import sqlalchemy
from etlstat.database.Database import Database

from etlstat.log.timing import log

LOG_LEVEL = 'INFO'

if LOG_LEVEL == 'DEBUG':
    log.basicConfig(level=log.DEBUG)
else:
    log.basicConfig(level=log.INFO)

log = log.getLogger(__name__)


class MySql(Database):

    def __init__(self, host, port, database, user, password):
        """ Constructor.

            Args:
                param1  (str):          Host
                param2  (str):          Port
                param3  (str):          Database

                param4  (str):          User
                param5  (str):          Password
        """
        super().__init__('mysql+mysqlconnector', host, port, database, user, password)

    def insert(self, table, row):
        """
        """
        string_row = row[0]
        for element in row:
            string_row += ","+element

        sql = """INSERT INTO {0} VALUES ({1})"""\
              .format(table, ', '.join(row))

        try:
            log.debug(" Running instruction: [ %s ]", sql)
            self.query(sql)
        except sqlalchemy.exc.SQLAlchemyError:
            log.error(" Cannot execute insert operation.")

    def select(self, table, params, conditions=""):
        sql = "SELECT {0} FROM {1}".format(params, table)

        if conditions:
            sql += " WHERE {0}".format(conditions)
        try:
            log.debug(" Running instruction: [%s]", sql)
            return self.query(sql)
        except sqlalchemy.exc.SQLAlchemyError:
            log.error(" Cannot execute select operation.")

    def select_all(self, table):
        """
        """
        sql = "SELECT * FROM {0}"\
              .format(table)

        try:
            log.debug(" Running instruction: [%s]", sql)
            return self.query(sql)
        except sqlalchemy.exc.SQLAlchemyError:
            log.error(" Cannot execute select_all operation.")

    def bulk_insert(self, data_frame, table, csv_path):
        """
        """
        data_frame.to_csv(csv_path, sep=';', header=False, index=False)

        sql = "LOAD DATA LOCAL INFILE '{0}' INTO TABLE {1} FIELDS TERMINATED BY ';'"\
              .format(csv_path, table)

        try:
            log.debug(" Running instruction: [%s]", sql)
            self.query(sql)
        except sqlalchemy.exc.SQLAlchemyError:  # sqlalchemy.exc.SQLAlchemyError
            log.error(" Cannot execute bulk_insert operation.")

    def update(self, table, params, conditions=""):
        sql = "UPDATE {0} SET {1}".format(table, params)

        if conditions:
            sql += " WHERE {0}".format(conditions)

        try:
            log.debug(" Running instruction: [%s]", sql)
            return self.query(sql)
        except sqlalchemy.exc.SQLAlchemyError:
            log.error(" Cannot execute update operation.")

    def delete(self, table):
        """
        """
        try:
            self.query("delete from " + table)
        except sqlalchemy.exc.SQLAlchemyError:
            log.error(" Cannot execute delete operation.")

    def check_for_table(self, table):
        return self._engine.dialect.has_table(self._engine, table)


def store_mysql(dataframe, host, port, database, table, user, password):
    """ Store rows of dataframe into MySql database table.

        Args:
            param1  (dataframe):    Dataframe with data rows.
            param2  (str):          Host
            param3  (str):          Port
            param4  (str):          Database
            param5  (str):          Table
            param6  (str):          User
            param7  (str):          Password
    """
    db_mysql = MySql(host, port, database, user, password)

    db_mysql.delete(table)
    
    db_mysql.bulk_insert(dataframe, table)

