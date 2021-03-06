from flattningfiles import FlatteningFiles
import pandas as pdf
from s3reader.s3jsonreader import JsonReader


class WasAssessmentSource(FlatteningFiles):
    assessment = None

    def convert_to_pdf(self, **kwargs):

        utility = kwargs.get("utils")  # get test execution parameters
        s3reader = JsonReader()
        json_content_list = s3reader.read_json(utility.was_bucket,
                                               "couchbase/assessment/year={}/month={}/day={}".format(utility.year,
                                                                                                     utility.month,
                                                                                                     utility.day))

        WasAssessmentSource.assessment = pdf.json_normalize(json_content_list, max_level=0, errors='ignore')

