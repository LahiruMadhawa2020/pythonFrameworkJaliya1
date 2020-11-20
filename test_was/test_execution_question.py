
from quicktest.quicktest import QuickTest
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.wasmastertarget import WasMasterTarget
from wasmaster.wasquestionsource import WasQuestionSource


#question_master = TbToValidate(QuickTest()).get_table(QuickTest.question_master)
#question_items = QuickTest.question_items

#TbToValidate(WasQuestionSource()).get_table(WasQuestionSource.question_master)

@pytest.fixture
def question_master_source(utility):
    question_master_source = TbToValidate(WasQuestionSource(), utils=utility).get_table(WasQuestionSource.question_master)
    return question_master_source


@pytest.fixture
def question_master_items_source():
    question_items_source = WasQuestionSource.question_items
    return question_items_source


@pytest.fixture
def question_master_target(redshift_db_connection, utility):
    question_master_target = TbToValidate(WasMasterTarget(), sql="""select
                                                                    numattempts
                                                                    ,id
                                                                    ,_doctype
                                                                    ,questioncardid
                                                                    ,"context.consumer_key"
                                                                    ,"context.master_assessment_id"
                                                                    ,"context.product_id"
                                                                    from was_question where year='{}'
                                                                and month='{}' and day='{}'""".format(utility.year,
                                                                                                      utility.month,
                                                                                                      utility.day), chunksize=0,dbconnection=redshift_db_connection).get_table(WasMasterTarget.pandaDf)
    return question_master_target


@pytest.mark.dependency(name="main-test_cp")
@pytest.mark.run(order=1)
def test_source_json_general(question_master_source):

    result = Validation().run_validation_on(question_master_source).expects_column_to_exist("id","presence Check -column id").\
                                                                   expects_column_to_exist("policies.attemptPolicy.maxAttempts","presence Check -column context.product_id").\
                                                                   expect_table_row_count_grater_than(0, "record count").get_results()


    # perform Pytestest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=2)
def test_source_json_business_check(question_master_source):

    # select if any records that version is null and version number grater not equal zero
    df_business_rule_1 = question_master_source[(question_master_source['numAttempts'] == 0) & (question_master_source['score.value'] > 0)]
    print(df_business_rule_1)
    # run validation
    result = Validation().run_validation_on(df_business_rule_1).expect_table_row_count(0, "business validation rule 1").get_results()

    # perform Pytest Assertion
    assert TestResult().is_test_passed(result) == 'True'


@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=3)
def test_source_target_values(question_master_source,question_master_target):

    # get the summation of number of attempt on both table and compare the result

    total_master_attempt_count = question_master_source[['numAttempts']].sum()
    total_master_attempt_terget = question_master_target[['numattempts']].sum()
    print(total_master_attempt_count)
    print(total_master_attempt_terget)
    result = Validation().expect_table1_value_to_be_equal_to_table2_value(total_master_attempt_count,total_master_attempt_terget,
                                                                 "master and items attempt count validation").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'

@pytest.mark.dependency(depends=["main-test_cp"])
@pytest.mark.run(order=4)
def test_source_target_table(question_master_source, question_master_target):

    colums_to_test = ["numattempts", "id", "_doctype", "questionCardId", "context.consumer_key",
                       "context.master_assessment_id","context.product_id"]

    # get the relevant columns of the both table and perform raw wise comparison
    master_question_attempt_source = question_master_source[['numAttempts'
                                                             , 'id'
                                                             , '_doctype'
                                                             , 'questionCardId'
                                                             , 'context.consumer_key'
                                                             , 'context.master_assessment_id'
                                                             , 'context.product_id']]
    master_question_attempt_source.rename(columns={'numAttempts': 'numattempts'}, inplace=True)
    master_question_attempt_target = question_master_target

    result = Validation().expect_table1_all_rows_overlap_with_table2(master_question_attempt_source,master_question_attempt_target,colums_to_test,
                                                                     "source and target raw validation").get_results()

    # perform PYTest Assertion
    assert TestResult().is_test_passed(result) == 'True'
