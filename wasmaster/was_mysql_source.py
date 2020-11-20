from flattningfiles import FlatteningFiles
import pandas as pdf


class WasMasterMysqlSource(FlatteningFiles):

    pandaDf = None

    """
     This function create pandas data frames connecting to MYSQL database
    """
    def convert_to_pdf(self, **kwargs):

        #connection = RedshiftConnector.get_connection()
        connection = kwargs.get("dbconnection")
        chunksize = int(kwargs.get("chunksize"))
        offset = 0
        sql = kwargs.get("sql")
        df = []

        if chunksize < 1:
            print("-----------before data from db-----")
            WasMasterMysqlSource.pandaDf = pdf.read_sql_query(sql=kwargs.get("sql"), con=connection, chunksize=None)
            print("-----------data from db-----")
            print(WasMasterMysqlSource.pandaDf)
        else:
            while True:
                sql = sql+" Limit {} offset {}".format(chunksize, offset)
                inter_df = pdf.read_sql_query(sql=kwargs.get("sql"), con=connection, chunksize=chunksize)
                print("---1--")
                print(type(inter_df))
                print("---2--")
                df.append(inter_df)
                offset += chunksize
                print(list(df[-1]))
                if len(list(df[-1])) < chunksize:
                    break
            WasMasterMysqlSource.pandaDf = pdf.concat(list(df))