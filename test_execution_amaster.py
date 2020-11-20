from wasmaster.waassignmentsource import WasAmasterSource
from wasmaster.wasmastertarget import WasMasterTarget
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from utils.utility import Utility


@pytest.fixture
def assessment_master_source(utility):
    amaster_source = TbToValidate(WasAmasterSource(), utils=utility).get_table(WasAmasterSource.amaster)
    return amaster_source


@pytest.fixture
def assessment_master_target(redshift_db_connection, utility):
    amaster_target = TbToValidate(WasMasterTarget(), sql="""SELECT id, productId, author, creationDate,version from public.was_amaster
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return amaster_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(assessment_master_source):

    result = Validation().run_validation_on(assessment_master_source).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_target_json_general(assessment_master_target):

    result = Validation().run_validation_on(assessment_master_target).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-target").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=3)
def test_source_json_business_check(assessment_master_source):
    # select if any records that version is null and version number grater not equal zero
    df_business_rule_1 = assessment_master_source[(assessment_master_source.version.notnull()) & (assessment_master_source.version != 0)]
    # run validation
    result = Validation().run_validation_on(df_business_rule_1).expect_table_row_count(0, "business validation rule 1").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=4)
def test_source_target(assessment_master_source,assessment_master_target):
    colums_to_test = ["id", "productId", "author", "creationDate", "version"]

    result = Validation().expect_table1_all_rows_overlap_with_table2(assessment_master_source,assessment_master_target,colums_to_test,"test_cp source and target table amaster").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'



