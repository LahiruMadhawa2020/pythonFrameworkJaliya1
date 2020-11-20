from redshifconnection.redshiftconnector import RedshiftConnector
from mysqlconnection.mysqlconnector import MysqlConnector
from validationrules.validation import Validation
import pytest
from utils.utility import Utility

@pytest.fixture(scope="session")
def redshift_db_connection():
    redshift_connection = RedshiftConnector.get_connection()
    yield redshift_connection
    RedshiftConnector.close_connection_pool()

    return redshift_connection


@pytest.fixture(scope="session")
def mysql_db_connection():
    mysql_connection = MysqlConnector.get_connection()
    yield mysql_connection
    MysqlConnector.close_connection_pool()

    return mysql_connection


@pytest.fixture(scope="session")
def utility():
    Utility.set_bucket_location()
    Utility.set_date_range()

    return Utility


