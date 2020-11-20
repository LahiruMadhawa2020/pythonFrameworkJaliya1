from flattningfiles import FlatteningFiles
from redshifconnection.redshiftconnector import RedshiftConnector
import pandas as pdf


class WasMasterTarget(FlatteningFiles):

    pandaDf = None

    """
     This function create pandas data frames connecting to Redshift database
    """
    def convert_to_pdf(self, **kwargs):

        #connection = RedshiftConnector.get_connection()
        connection = kwargs.get("dbconnection")
        chunksize = int(kwargs.get("chunksize"))
        offset = 0
        sql = kwargs.get("sql")
        dfgenerators = []
        df =[]
        temp_df =None
        inter_df = []

        if chunksize < 1:
            WasMasterTarget.pandaDf = pdf.read_sql_query(sql=kwargs.get("sql"), con=connection, chunksize=None)
        else:
            inter_df = pdf.read_sql_query(sql=kwargs.get("sql"), con=connection, chunksize=chunksize)
            WasMasterTarget.pandaDf = pdf.concat(inter_df)
