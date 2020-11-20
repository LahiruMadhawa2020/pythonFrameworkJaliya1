from quicktest.testcsv import Testcsv
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.wasmastertarget import WasMasterTarget
from utils.utility import Utility


@pytest.fixture
def csv_source(utility):
    test_source = TbToValidate(Testcsv(), utils=utility).get_table(Testcsv.test_date)
    print(test_source)
    return test_source


@pytest.fixture
def assessment_master_target(redshift_db_connection, utility):
    amaster_target = TbToValidate(WasMasterTarget(), sql="""SELECT id, productId, author, creationDate,version from public.was_amaster
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return amaster_target


@pytest.mark.run(order=1)
def test_source_csv_general(csv_source):



    result = Validation().run_validation_on(csv_source).expect_column_values_to_be_unique("ID", "Test- Unique value")\
                                                       .expect_column_values_to_not_be_null("ID", "Test- Not Null")\
                                                       .expect_column_value_lengths_to_equal("Name", 4, "Length 4 ")\
                                                       .expect_column_values_to_be_of_type("Name", "object", "data type String only")\
                                                       .expect_column_values_to_be_in_set("Gender", ["M", "F"], "Values in List")\
                                                       .expect_column_values_to_match_regex("Email","(^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\\.[a-zA-Z0-9-.]+$)","email format check")\
                                                       .get_results()


    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.run(order=2)
def test_source_csv_business_validation(csv_source):
    csv_source_selection = csv_source[(csv_source.Age > 75)]

    result = Validation().run_validation_on(csv_source_selection).expect_table_row_count(0, "business validation -1")\
                                                       .get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'

@pytest.mark.run(order=3)
def test_target(assessment_master_target):
    sql = """SELECT id, productId, author, creationDate,version from public.was_amaster
                   WHERE year='{}' AND month='{}' AND DAY='{}'""".format(Utility.year,
                                                                         Utility.month,
                                                                         Utility.day)
    print(sql)
    print("---------1--------")
    print(assessment_master_target)
    print("---------2--------")
    assert 0