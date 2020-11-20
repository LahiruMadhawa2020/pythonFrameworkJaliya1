from quicktest.quicktest import QuickTest


question_df = QuickTest().read_test_json("/filerepo/question")


print(question_df.count())

#question_df[question_df['question_df']>0 and question_df['question_df']]