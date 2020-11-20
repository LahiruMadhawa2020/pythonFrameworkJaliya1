from flattningfiles import FlatteningFiles
import pandas as pdf
from s3reader.s3csvreader import CsvReader
from localdir.localfilereader import LocalFileReader
from io import StringIO


class CpProductSource(FlatteningFiles):

    cp_product = None

    def convert_to_pdf(self, **kwargs):
        csv_reader = CsvReader()

        csv_content_list = csv_reader.read_csv("aws-nonprod-datalake-cp-csv-product-wpng-analytics-dev", "")
        cp_product = pdf.read_csv(StringIO(csv_content_list), sep=",", skip_blank_lines=True)  # convert string to pandas data frame

        # print(initial_df)
        # csv_reader = LocalFileReader().read_csv("filerepo/csv")
        # initial_df = pdf.read_csv(StringIO(csv_reader), sep=",", skip_blank_lines=True,encoding='utf-8')  # convert string to pandas
        # initial_df.columns = ["Sequence", "Start", "End", "Coverage","t","jk"]
        # print(initial_df)