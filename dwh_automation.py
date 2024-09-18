# import qa_dl_count as qa
import json

class DWH_Queries:
    def __init__(self) -> None:
        
        # path = f"/Workspace/Shared/QA_AUTOMATION/json_files/{file_nm}.json"
        # with open(path,'r') as f:
        #     self.data = json.load(f)
        pass

    def customer_check(self,cust_array ):
        cust_to_number = {
            "KL": 14,
            "LM": 13,
            "PGR": 11,
            "SAIS": 9,
            "USAA": 17,
            "NGIC": 24,
            "UNFY": 35,
            "BLAG": 22,
            "LMX": 38,
            "BETA": 36,
            "COMP": 39
        }
        assigned_numbers = [cust_to_number[code] for code in cust_array]
        return assigned_numbers
    
    def striper(self,cols):
        any_col = ''
        for i in cols:
            any_col += i+','
        return any_col[:-1] 
    
class Main():
    def __init__(self) -> None:
        self.obj = DWH_Queries()
        pass

    def bk_validation(self,tab_nm,bk_cols):
        bk_col = self.obj.striper(bk_cols)
        bk_validn_qry = f""" select count(1) from adp_core.dwh.{tab_nm} group by {bk_col} having count(1)>1 """ 
        # print(bk_validn_qry)
        return spark.sql(bk_validn_qry)
    def pk_validation(self,tab_nm,pk_cols):
        pk_col = self.obj.striper(pk_cols)
        pk_validn_qry = f""" select count(1) from adp_core.dwh.{tab_nm} group by {pk_col} having count(1)>1 """ 
        # print(pk_validn_qry)
        return spark.sql(pk_validn_qry)

    def specific_area(self,feed_json,bk_cols,sub_subject):
        print("Hello")
        for sub_subject_area in feed_json.keys():

            cust = feed_json[f'{sub_subject_area}']['cust']
            src_qry = feed_json[f'{sub_subject_area}']['src_qry']
            tgt_qry = feed_json[f'{sub_subject_area}']['tgt_qry']
            # print(sub_subject_area)
            if sub_subject_area == 'tblagents' :
                cust = [22]
                self.qry_executor(src_qry, tgt_qry, cust, bk_cols)
    
class OPS(DWH_Queries):
    def __init__(self) -> None:
        # super().__init__()
        # obj_qry = DWH_Queries() 
        # obj_main = Main()
        # input_json = obj_qry.data[tab_nm]
        pass
    
    def ops_1(self, tab_nm, data):
        # obj_qry = DWH_Queries() 
        obj_main = Main()
        input_json = self.data[tab_nm]
        # print(input_json,tab_nm)
        try :
            status = 'NO'  if obj_main.pk_validation(tab_nm,tuple(input_json['pk'].values())).count() == 0 else 'YES' 
            status = 'NO'  if obj_main.bk_validation(tab_nm,tuple(input_json['bk'].values())).count() == 0 else 'YES' 
            values = f" ('DWH', '{tab_nm}', '{1}' , '{status}')"
            print(values)
            return values
        except :
            values = f" ('DWH', '{tab_nm}', '{1}' , 'raised_exception')"
            return values
    def iter_tables(self):
        ls = []
        subject_area = ['agent','policy','workflow','entity','finance']
        for f in subject_area:
            path = f"/Workspace/Shared/QA_AUTOMATION/json_files/{f}_module.json"
            with open(path,'r') as f:
                self.data = json.load(f)
                print(path)
            for t in self.data.keys():
                ls.append(self.ops_1(t,self.data))
                print(t)
            spark.sql(f"insert into adp_core.test.pk_DL_validation VALUES {','.join(ls)}")
            print('inserted..')

if __name__ == "__main__":
    obj_ops = OPS()
    obj_ops.iter_tables()