from wasmaster.wasquestionmastersource import QuestionMasterSource
from wasmaster.wasmastertarget import WasMasterTarget
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest


@pytest.fixture
def question_master_source(utility):
    question_mas_source = TbToValidate(QuestionMasterSource(), utils=utility).get_table(QuestionMasterSource.question_master)
    return question_mas_source

@pytest.fixture
def question_master_target(redshift_db_connection, utility):
    question_mas_target = TbToValidate(WasMasterTarget(), sql="""SELECT _doctype,difficulty,hasparts,id,name,version from public.was_qmaster
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return question_mas_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(question_master_source):

    result = Validation().run_validation_on(question_master_source).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_source_target(question_master_source,question_master_target):
    colums_to_test = ["_doctype", "difficulty", "hasparts", "id", "name", "version"]

    result = Validation().expect_table1_all_rows_overlap_with_table2(question_master_target, question_master_target, colums_to_test,"test_cp source and target table question Master").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'