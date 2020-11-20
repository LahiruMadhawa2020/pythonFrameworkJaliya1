from flattningfiles import FlatteningFiles
import pandas as pdf
from s3reader.s3jsonreader import JsonReader


class WasQuestionSource(FlatteningFiles):

    question_master = None
    question_items = None

    def convert_to_pdf(self, **kwargs):
        utility = kwargs.get("utils")  # get test execution parameters
        s3reader = JsonReader()
        json_content_list = s3reader.read_json(utility.was_bucket,
                                               "couchbase/question/year={}/month={}/day={}".format(utility.year,
                                                                                                   utility.month,
                                                                                                   utility.day))
        # print(json_content_list)
        # question_master = json_content_list
        WasQuestionSource.question_master = pdf.json_normalize(json_content_list, max_level=5, errors='ignore')
        # WasQuestionSource.question_items = pdf.json_normalize(json_content_list, 'itemResults', ['id'], errors='ignore', max_level=5,
                                                        # record_prefix='items.')
