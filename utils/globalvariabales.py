import pandas as pdf


class GlobalVariables:

    # expect result storage variable
    geresult = []

    # Create the pandas DataFrame to
    data = [['True']]
    df = pdf.DataFrame(data, columns=['test_cp'])

    # test case id
    test_case_id = None

    # current year, month, date
    year = ""
    month = ""
    day = ""

    @staticmethod
    def _set_geresult(var):
        GlobalVariables.geresult.append(var)

    @staticmethod
    def _get_geresult():
        return GlobalVariables.geresult

    @staticmethod
    def _get_testpdf():
        return GlobalVariables.df

    @staticmethod
    def _set_test_case_id(var):
        GlobalVariables.test_case_id = var

    @staticmethod
    def _get_test_case_id():
        return GlobalVariables.test_case_id
