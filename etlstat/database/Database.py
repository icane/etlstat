"""

"""
import mysql.connector.errors
import sqlalchemy
from sqlalchemy import exc, create_engine

from etlstat.log.timing import log

LOG_LEVEL = 'INFO'
if LOG_LEVEL == 'DEBUG':
    log.basicConfig(level=log.DEBUG,
                    format='%(asctime)s :: %(levelname)-7s %(message)s',
                    datefmt='%m-%d %H:%M')
else:
    log.basicConfig(level=log.INFO)

log = log.getLogger(__name__)


class Database(object):

    _engine = None
    _connection = None
    _engine_type = ""
    _user = ""
    _password = ""
    _server = ""
    _database = ""
    _port = ""

    state = False

    def __init__(self, engine_type, host, port, database, user, password):
        """ Constructor.

            Args:
                param1  (str)   Engine type
                param2  (str)   host
                param3  (str)   port
                param4  (str)   database or service name (Oracle)
                param5  (str)   user
                param6  (str)   password
        """

        self._engine_type = engine_type
        self._server = host
        self._port = port
        self._database = database
        self._user = user
        self._password = password

        self.create_engine()
        self.connect()

    def __del__(self):
        self.close()

    def __exit__(self):
        self.close()

    def create_engine(self):
        """
        """
        log.debug(" Building engine...")

        if self._engine_type == "oracle+cx_oracle":
            url = "{0}://{1}:{2}@{3}:{5}/?service_name={4}"
        else:
            url = "{0}://{1}:{2}@{3}:{5}/{4}"

        url_str = url.format(
            self._engine_type,
            self._user,
            self._password,
            self._server,
            self._database,
            self._port)

        self._engine = create_engine(url_str)

        log.debug(" Engine created.")

    def connect(self):
        """
        """
        try:
            self._connection = self._engine.connect()
            self.state = True
            log.debug(" Engine connected to database [{0}].".format(self._database))
        except sqlalchemy.exc.SQLAlchemyError as error:
            log.error(" Connection to database failed: " + str(error))
        except mysql.connector.errors.ProgrammingError as error:
            log.error(str(error))

    def query(self, sql):
        """
        """
        return self._engine.execute(sql)

    def close(self):
        """
        """
        if self.state:
            self._connection.close()
            self._engine.dispose()
            self.state = False
            log.debug(" Database connection closed.")
            log.debug(" Engine closed.")
        else:
            log.error(" There is not connection to close.")
