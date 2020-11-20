from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.wasassessmentsource import WasAssessmentSource
from wasmaster.wasmastertarget import WasMasterTarget

@pytest.fixture
def assessment_source(utility):
    asse_source = TbToValidate(WasAssessmentSource(), utils=utility).get_table(WasAssessmentSource.assessment)
    return asse_source

@pytest.fixture
def assessment_target(redshift_db_connection, utility):
    asse_target = TbToValidate(WasMasterTarget(), sql="""SELECT id,_doctype,score,duedate,"context.user_id",
                                                          "context.product_id",title,creationdate from public.was_assessment
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return asse_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(assessment_source):

    result = Validation().run_validation_on(assessment_source).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_source_target(assessment_source,assessment_target):
    colums_to_test = ["id", "_doctype", "score", "duedate", "context.user_id", "context.product_id",
                      "context.product_id", "title", "creationdate"]

    result = Validation().expect_table1_all_rows_overlap_with_table2(assessment_source,assessment_target,colums_to_test,"test_cp source and target table assessment").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'
