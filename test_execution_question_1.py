
from wasmaster.wasmastertarget import WasMasterTarget
from wasmaster.wasquestionsource import WasQuestionSource
from tablecreation import TbToValidate
from validationrules.validation import Validation
from validationrules.testresult import TestResult
import pytest
from wasmaster.was_mysql_source import WasMasterMysqlSource
from wasmaster.waassignmentsource import WasAmasterSource
from utils.utility import Utility

print(Utility.set_date_range().year)
print(Utility.set_date_range().month)
print(Utility.set_date_range().day)
# @pytest.fixture
# def target_assessment_table(mysql_db_connection):
#     #question_master = TbToValidate(WasQuestionSource()).get_table(WasQuestionSource.question_master)
#     #question_items = WasQuestionSource.question_items
#     question_items = TbToValidate(WasMasterMysqlSource(), sql="""select count(*) as count from external_user""", dbconnection=mysql_db_connection,chunksize=0).get_table(WasMasterMysqlSource.pandaDf)
#     return question_items
#
#
# def test_source_json_general(target_assessment_table):
#
#     result = Validation().run_validation_on(target_assessment_table).\
#                                                        expect_table_row_count_grater_than(1, "record count").get_results()
#                                                              #expects_column_to_exist("policies.attemptPolicy.maxAttempts","test_cp column -maxAttempts").\
#
#
#
#     # perform PYTest Assertion
#     assert TestResult().is_test_passed(result) == 'True'


# def test_source_json_business_check():
#     # select if any records that version is null and version number grater not equal zero
#
#     df_business_rule_1 = question_master[(question_master['numAttempts'] == 0) & (question_master['score.value'] > 0)]
#     print(df_business_rule_1)
#     # run validation
#     result = Validation().run_validation_on(df_business_rule_1).expect_table_row_count(0, "business validation rule 1").get_results()
#
#     # perform PYTest Assertion
#     assert TestResult().is_test_passed(result) == 'True'
#
# @pytest.mark.run(order=3)
# def test_source_target_values():
#     total_master_attempt_count = question_master[['numAttempts']].count()
#     #total_item_attempt_count = question_items[['items.numAttempts']].count()
#     total_item_attempt_count = question_master[['numAttempts']]
#
#     result = Validation().expect_table1_value_to_be_equal_to_table2_value(total_master_attempt_count,total_item_attempt_count,
#                                                                  "master and items attempt count validation").get_results()
#
#     # perform PYTest Assertion
#     assert TestResult().is_test_passed(result) == 'True'

#test_source_target_values()
