from concurrent.futures import ThreadPoolExecutor
from datetime import datetime,timedelta as tm
from databricks import sql as DB
from pytz import timezone
import concurrent
import pandas as pd
import threading
import warnings
import pymssql
import time

warnings.filterwarnings('ignore')



class COUNT_CHECK():
    def __init__(self) -> None:
        self.ssms_user='analytics-prod-etl'
        self.ssms_password='nUncog07eNfeLYAW'
        self.host="bolt-analyticsprod.cloud.databricks.com"
        self.http_path= "sql/protocolv1/o/3158924937735331/0929-073844-guj0msl8"
        self.access_token='dapi5894ba12237c3ccbc1c3761f2795c505'#'{{ conn.databricks_default.password }}' #"" 
        ind_time = datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S')
        print(f"{self.http_path}  |  {self.access_token}")
        self.odbc_driver='ODBC Driver 17 for SQL Server'
        self.given_date = datetime.now()
        # self.# previous_day = given_date - tm(days=1)
        self.formatted_date = self.given_date.strftime("%Y-%m-%d")
        print("Previous Day:", self.formatted_date )
        self.business_end_dt=self.formatted_date+" 00:00:000"
        self.business_start_dt='1900-01-01'
        self.min_date='1900-01-01'
        self.num_threads = None
        self.config_df = None
        self.threads= []
        self.cnt_conxn = None
        self.df_tgt = []
        self.df_src = []
        self.cfg_df = None
        

    def local_db(self) :
        connection = DB.connect(
        server_hostname=self.host,
        http_path=self.http_path,
        access_token=self.access_token)
        return connection
    
    def qry_executor(self) :
        self.local_db = self.local_db()
        self.query = f"""select config_num,customer_id from adp_core.admin.cfg_configuration_details where config_entity_name = 'SUBSCRIPTION GROUP' """
        self.cfg_df  = pd.read_sql(self.query, self.local_db)
        obj = QUERIES(self)
        # self.local_db = self.local_db()
        self.all_values = pd.read_sql(obj.mst_qry, self.local_db)
        # df_new = []
        for _,row in self.all_values.iterrows():
                # dataBricks_qry(
                table_name = row['table_name']
                customer_id = row['customer_id']
                subject_area = row['subject_area']
                source_table = row['source_table']
                server_con = row['conxn']
                db = row['DB']
                formatted_date = self.given_date
                tgt_query=f"""select count(*) as cnt ,'{table_name}' as tbl ,'{customer_id}' as cust,\n '{subject_area}' as sub_area,'{formatted_date}' as executed_on ,'{source_table}' as src_tbl 
                       from adp_core.dl{customer_id}.{table_name} where active_flag='YES'"""
                src_qry = obj.ssms_qry(table_name = row['table_name'], customer_id = row['customer_id'], source_table = row['source_table'], 
                         conxn = row['conxn'], db = row['DB'], target_dt_created = row['target_dt_created'], target_dt_updated = row['target_dt_updated'], case_flag = row['case_flag'], left_on = row['left_on'],
                         business_start_dt = self.business_start_dt, business_end_dt = self.business_end_dt)
                ssms_cnxn=pymssql.connect(server_con, self.ssms_user, self.ssms_password)
                try :
                    src_df = pd.read_sql(src_qry, ssms_cnxn)
                    tgt_df = pd.read_sql(tgt_query, self.local_db)
                    lis = pd.merge(left=src_df, right=tgt_df, 
                            how='left', left_on=['tgt_table','cust'],right_on= ['tbl','cust'] )
                    # lis = self.cfg_qry(src_df,cfg_df,customer_id)
                    for _,r in lis.iterrows():
                        values = f" ('{r['cust']}', '{r['tgt_table']}', '{r['cnt_x']}', '{r['cnt_y']}','{r['sub_area']}', '{r['src_tbl']}', '{r['executed_on']}') "
                    pd.read_sql_query(f"insert into adp_core.test.count_validation4 VALUES {values}",self.local_db)
                    # print(f"""{lis}"->"{table_name}""" )
                    # print(f"""{server_con}
                    #       tgt_qry->{tgt_query}
                    #       src_qry"->{src_qry}""")
                except Exception as e :
                    print(f"""{server_con}
                          tgt_qry->{tgt_query}
                          src_qry"->{src_qry} \n """)
                    print(e)
                    pass
                print(_)
        print("inserted...")#some_push

class QUERIES(COUNT_CHECK):
    def __init__(self,instance):
        self.business_start_dt='1900-01-01'
        self.mst_qry = f"""select a.* from adp_core.test.config_table_qa a  where  a.case_flag <> 'Disbled'
                            """
        self.obj = COUNT_CHECK()
        # self.query = f"""select config_num,customer_id from adp_core.admin.cfg_configuration_details where config_entity_name = 'SUBSCRIPTION GROUP' """
        # self.cfg_df  = pd.read_sql(self.query, self.obj.local_db())
        self.cfg_df = instance.cfg_df
    
    def cfg_config_nums(self,cust):
        cfg_val=  self.cfg_df.query(f'customer_id == {cust}')
        df = cfg_val.loc[:,'config_num'].tolist()
        # print(type(df))
        if len(df)==1 : return  f"({df[0]})"
        else : return df[0] ,df[1]
    
    def ssms_qry(self,**kwargs):
        left_on = 'Agentno' if kwargs.get('left_on') == None else kwargs.get('left_on')
        target_dt_created = dt_created = kwargs.get('target_dt_created')
        target_dt_updated = dt_updated = kwargs.get('target_dt_updated')
        target_tbl = table_name = kwargs.get('table_name')
        customer_id = kwargs.get('customer_id')
        source_table = kwargs.get('source_table')
        DB = kwargs.get('db')
        business_start_dt = kwargs.get('business_start_dt')
        business_end_dt = kwargs.get('business_end_dt')
        case_flag = kwargs.get('case_flag')
        if dt_created == None and dt_updated == None :
            if case_flag == 'CFG':
                query=f"select count(*) as cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\n from {DB}..[{source_table}] A JOIN SAIS_Producers..tblAgents B ON left(A.{left_on},5)=B.AgentNo and b.subscription_group in {self.cfg_config_nums(customer_id)}  \n"
                return query
            query=f"select count(1) as cnt,'{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\n from {DB}..[{source_table}]  \n"
            return query
        if dt_updated == None :
            if case_flag == 'CFG':
                query=f"""select count(*) as cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\n from {DB}..[{source_table}]  A JOIN SAIS_Producers..tblAgents B 
                ON left(A.{left_on},5)=B.AgentNo and b.subscription_group in {self.cfg_config_nums(customer_id)}  where a.{target_dt_created} <= cast('{business_end_dt}' as datetime) and a.{target_dt_created} >=  cast ('{business_start_dt}' as datetime) or a.{target_dt_created} is null \n """
                return query
            query = f"""select count(*) cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\nfrom {DB}..[{source_table}] where {target_dt_created} <= cast('{business_end_dt}' as datetime) and {target_dt_created} >=  cast ('{business_start_dt}' as datetime) or {target_dt_created} is null  \n """
            return query
        else :
            if case_flag == 'CFG':
                query=f"""select count(*) as cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\n from {DB}..[{source_table}] A JOIN SAIS_Producers..tblAgents B ON left(A.{left_on},5)=B.AgentNo 
                         and b.subscription_group in {self.cfg_config_nums(customer_id)}
                         where  ((a.{target_dt_created} >=  cast ('{business_start_dt}' as datetime) and a.{target_dt_created}  <=  cast ('{business_end_dt}' as datetime))
                         or (a.{target_dt_updated} >= cast('{business_start_dt}' as datetime) and a.{target_dt_updated} <= cast('{business_end_dt}' as datetime))) 
                         or a.{target_dt_created} is null  or a.{target_dt_updated} is null  \n"""
                return query
            query = f"""select count(*) cnt, '{table_name}' as tbl ,'{customer_id}' as cust,'{target_tbl}' as tgt_table\nfrom {DB}..[{source_table}]
                         where  (({target_dt_created} >=  cast ('{business_start_dt}' as datetime) and {target_dt_created}  <=  cast ('{business_end_dt}' as datetime))
                         or ({target_dt_updated} >= cast('{business_start_dt}' as datetime) and {target_dt_updated} <= cast('{business_end_dt}' as datetime)))
                         or {target_dt_created} is null  or {target_dt_updated} is null  \n"""    
            return query


# if __name__ == '__main__':
#     COUNT_CHECK().qry_executor()