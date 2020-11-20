from sqlalchemy import create_engine
import config

class MysqlConnector:

    connection = None
    cursor = None
    mysql_engine = None

    @staticmethod
    def get_connection():

        host = getattr(config, 'mysql_host')
        password = getattr(config, 'mysql_password')
        user_name = getattr(config, 'mysql_user_name')
        mysql_schema= getattr(config, 'mysql_schema')

        try:

            MysqlConnector.mysql_engine.red_engine = create_engine(
                'mysql+mysqldb://{}:{}@{}:5439/{}'.format(user_name, password, host,mysql_schema))
            MysqlConnector.mysql_engine.connection = MysqlConnector.mysql_engine.red_engine.connect()

        except (Exception) as error:
            print("Error while connecting to mysql", error)

        return MysqlConnector.connection

    @staticmethod
    def close_connection_pool():
        print("closing redshift db connection")
        MysqlConnector.mysql_engine.dispose()