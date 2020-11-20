from contentplayer.cpproductsource import CpProductSource
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.wasmastertarget import WasMasterTarget


@pytest.fixture(scope="session")
def setup_module():
    print("set up")


@pytest.fixture(scope="session")
def teardown_module():
    Validation.generate_report()


@pytest.fixture
def cp_product_source():
    cp_prod_source = TbToValidate(CpProductSource()).get_table(CpProductSource.cp_product)
    return cp_prod_source


@pytest.fixture
def question_master_target(redshift_db_connection):
    question_master_target = TbToValidate(WasMasterTarget(), sql="""select
                                                                    numattempts
                                                                    ,id
                                                                    ,_doctype
                                                                    ,questioncardid
                                                                    ,"context.consumer_key"
                                                                    ,"context.master_assessment_id"
                                                                    ,"context.product_id"
                                                                    from was_question where year='2020'
                                                                and month='09' and day='15' order by id""", chunksize=1000, dbconnection=redshift_db_connection).get_table(WasMasterTarget.pandaDf)
    return question_master_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_csv_general(cp_product_source):

    result = Validation().run_validation_on(cp_product_source).expect_table_row_count_grater_than(0, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=2)
def test_target_table_general(question_master_target):

    result = Validation().run_validation_on(question_master_target).expect_table_row_count(59620, "raw count validation-source").\
                                                                      get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'