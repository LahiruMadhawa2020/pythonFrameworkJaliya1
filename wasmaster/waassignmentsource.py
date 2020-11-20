from flattningfiles import FlatteningFiles
import pandas as pdf
import json
from s3reader.s3jsonreader import JsonReader
from pandas.io.json import json_normalize


class WasAmasterSource(FlatteningFiles):

    amaster = None
    amsterItems = None

    def convert_to_pdf(self, **kwargs):

        utility = kwargs.get("utils")  # get test execution parameters
        s3reader = JsonReader()  # initialization of jSON reader class
        json_content_list = s3reader.read_json(utility.was_bucket,
                                               "couchbase/amaster/year={}/month={}/day={}".format(utility.year,
                                                                                                utility.month,
                                                                                                utility.day))

        WasAmasterSource.amaster = pdf.json_normalize(json_content_list, max_level=0, errors='ignore')[
            ["id", "productId", "author", "creationDate", "version"]]
        # WasAmasterSource.amsterItems = pdf.json_normalize(json_content_list, max_level=0, errors='ignore')[
        #     ["id", "productId", "author", "creationDate"]]
