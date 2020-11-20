import config
from sqlalchemy import create_engine


class RedshiftConnector:

    connection = None
    cursor = None
    red_engine = None

    @staticmethod
    def get_connection():

        host = getattr(config, 'host')
        password = getattr(config, 'password')
        user_name = getattr(config, 'user_name')

        try:

            RedshiftConnector.red_engine = create_engine(
                'redshift+psycopg2://{}:{}@{}:5439/nextgen'.format(user_name, password, host))
            RedshiftConnector.connection = RedshiftConnector.red_engine.connect()

        except (Exception) as error:
            print("Error while connecting to Redshift", error)

        return RedshiftConnector.connection

    @staticmethod
    def close_connection_pool():
        print("closing redshift db connection")
        RedshiftConnector.red_engine.dispose()
