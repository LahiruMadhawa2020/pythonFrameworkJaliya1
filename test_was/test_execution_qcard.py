from wasmaster.wasqcardource import QCardSource
from wasmaster.wasmastertarget import WasMasterTarget
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest


@pytest.fixture
def qcard_master_source(utility):
    qcard_source = TbToValidate(QCardSource(), utils=utility).get_table(QCardSource.qCard)
    return qcard_source

@pytest.fixture
def qcard_target(redshift_db_connection, utility):
    qcard_target = TbToValidate(WasMasterTarget(), sql="""SELECT _doctype,id,mqid,title from public.was_qcard
                WHERE year='{}' AND month='{}' AND DAY='{}'""".format(utility.year,
                                                                      utility.month,
                                                                      utility.day), dbconnection=redshift_db_connection, chunksize=0).get_table(WasMasterTarget.pandaDf)
    return qcard_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(qcard_master_source):

    result = Validation().run_validation_on(qcard_master_source).expects_column_to_exist("id", "presence Check column-Id").\
                                                                      expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_source_target(qcard_master_source,qcard_target):
    colums_to_test = ["_doctype", "id", "mqid", "title"]

    result = Validation().expect_table1_all_rows_overlap_with_table2(qcard_master_source, qcard_target, colums_to_test,"test_cp source and target table qcard").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'