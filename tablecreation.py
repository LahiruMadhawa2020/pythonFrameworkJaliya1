

class TbToValidate:

    __pdf = None

    def __init__(self, object, **kwargs):
       keyargumets=kwargs
       object.convert_to_pdf(**keyargumets)

    def get_table(self, pdf):
        self.__pdf = pdf
        return self.__pdf
