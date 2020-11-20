import uuid
import json
import pandas as pdf
import json
from utils.globalvariabales import GlobalVariables as gbvar
from IPython.display import display_html
from datetime import datetime, timedelta
import configparser
import sys
import os


class Utility:

    output = {
    "validation_id": "",
    "test_case_id": "",
    "test_case_name":"",
    "comparison_report": "",
    "exception": "",
    "df1_unique_columns":"",
    "df2_unique_columns": ""
    }

    # date fragmentation
    year = ""
    month = ""
    day = ""

    # bucket
    was_bucket = ""


    @staticmethod
    def generate_uuid():
        return "avantador-"+str(uuid.uuid4())

    @staticmethod
    def generate_meta_info(**kwargs):
        Utility.output["validation_id"] = str(kwargs.get("validation_id"))
        Utility.output["test_case_id"] = str(kwargs.get("test_case_id"))
        Utility.output["test_case_name"] = str(kwargs.get("test_case_name"))
        Utility.output["comparison_report"] = str(kwargs.get("comparison_report"))
        Utility.output["exception"] = str(kwargs.get("exception"))
        Utility.output["df1_unique_columns"] = str(kwargs.get("df1_unique_columns"))
        Utility.output["df2_unique_columns"] = str(kwargs.get("df2_unique_columns"))

        meta_json_object = json.dumps(Utility.output)
        return meta_json_object

    @staticmethod
    def get_table_columns(table):
        list=[]
        for column in table.columns:
            list.append(column)
        return list

    @staticmethod
    def generate_report():
        repo = pdf.json_normalize(gbvar._get_geresult(), max_level=3, errors='ignore')
        display_html(repo)
        repo.to_html('test_result_in_detail.html')

    @staticmethod
    def set_date_range():

        print(Utility.get_file_path("execution_config.ini"))
        delta = Utility.__read_config_files(Utility.get_file_path("execution_config.ini"), "DATE-FRAME", "backdate_count")  # Getting backdated value

        now = datetime.now() - timedelta(int(delta))

        def converter(val): return "0"+str(val) if len(str(val)) == 1 else val  # function to modify date month to 2 figure value (s3 partition)

        Utility.year = str(now.year)
        Utility.month = str(converter(now.month))
        Utility.day = str(converter(now.day))

        return Utility

    @staticmethod
    def set_bucket_location():
        print(Utility.get_file_path("execution_config.ini"))
        buck_name = Utility.__read_config_files(Utility.get_file_path("execution_config.ini"), "WAS BUCKET", "bucket")  # Getting bucket name
        Utility.was_bucket = buck_name

    @staticmethod
    def __read_config_files(filename, frame, attribute):
        config_object = configparser.ConfigParser()
        config_object.read(filename)
        print(config_object.sections())
        data_frame = config_object[frame]  # Get the back date count
        return data_frame[attribute]


    @staticmethod
    def get_file_path(filename):

        parent_dir = os.getcwd()  # find the path to module a
        # Then go up one level to the common parent directory
        path = os.path.dirname(parent_dir)
        # Add the parent to sys.pah
        #print(sys.path.append(path))
        return path + "/" +filename