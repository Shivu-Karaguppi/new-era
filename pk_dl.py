import qa_dl_count as qa
import pandas as pd 
from databricks import sql as DB 

class PK_VALDN:
    def __init__(self) -> None:
        pass
    
    def conn(self):
        self.host="bolt-analyticsprod.cloud.databricks.com"
        self.http_path= "#http_path_databricks"
        self.access_token='#access token'
        connection = DB.connect(
        server_hostname=self.host,
        http_path=self.http_path,
        access_token=self.access_token)
        return connection
    def queries():
        file_r = pd.read_excel(r"C:\Users\shivanandk\Desktop\podman\AirflowCode\dags\qa_automation_new\PK_validation_DL.xlsx")
        print(file_r)

    def qry_executor(self) :
        self.local_db = qa.COUNT_CHECK().local_db()
        # self.query = f"""
        print(self.local_db)

PK_VALDN().conn()
