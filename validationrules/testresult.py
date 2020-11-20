
class TestResult:

    def is_test_passed(self, result):
        status = result[['success']].values.tolist()
        print(status)
        if [False] in status:
            print("im in false")
            return 'False'
        else:
            print("im in true")
            return 'True'

