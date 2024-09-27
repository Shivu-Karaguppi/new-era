# import qa_dl_count as qa
import json

class DWH_Queries:
    def __init__(self) -> None:
        
        path = r"/Workspace/Shared/QA_AUTOMATION/json_files/agt_module.json"
        with open(path,'r') as f:
            self.data = json.load(f)

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
    
    def give_bk_cols(self,t_cols, s_cols,bk_cols):
        # print(t_cols)
        # print(s_cols)
        t_col = [t_cols[t_cols.index(v)] for v in bk_cols if v in t_cols]
        s_col = [ s_cols[t_cols.index(v)] for v in bk_cols if v in t_cols]
        return (t_col,s_col)



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

    def data_completness(self, src_qry, tgt_qry,bk_cols):
        qry = f""" ({tgt_qry}) except ({src_qry}) 
            """
        execp_df = spark.sql(qry)
        zero_rec = execp_df.count()
        if zero_rec == 0:
            print("Data is complete & Perfect :-)")
        else :
            print("wow")
            # print(qry)
            self.fetch_ids(execp_df,bk_cols,src_qry,tgt_qry)

    def s_t_data_completness(self, src_qry, tgt_qry, t_cols, s_cols,bk_cols):
        print("batman")
        # print("Target table is missing rows from target table")
        # all_t_s_cols = self.obj.give_bk_cols( t_cols, s_cols,bk_cols)
        # try:
        #         qry = f"""select {self.obj.striper(all_t_s_cols[1])} from ({src_qry}) except select {self.obj.striper(all_t_s_cols[0])} from ({tgt_qry})
        #                     """
        #         print(qry)
        #         except_df = spark.sql(qry)
        #         print("these records are missing in tgt....") #insert record id into final table
        #         #################### Need to add some future code here ################################                
        # except Exception as e:
        #     #################### Need to add some future code here ################################
        #     print(e)
        #     pass
        pass
    
    def fetch_ids (self,execp_df,bk_cols,src_qry,tgt_qry):
        print(bk_cols)
        bks = []
        for bk in bk_cols:
            if bk not in ['CUSTOMER_ID', 'AUDIT_SYS_ID', 'AUDIT_TABLE_ID']: #, 'AUDIT_SRC_ID'
                bks.append(bk)
        bks = tuple(bks)
        dict_of_ids_and_col_nms = execp_df.select(*bks).toPandas().to_dict()
        self.data_validation(dict_of_ids_and_col_nms,src_qry,tgt_qry)

    def data_validation(self,dict_kv,src_qry,tgt_qry):
        last_key = ''
        for k,v in dict_kv.items():
            for k1,v1 in v.items():
                nth_where = f"""{k}= '{v1}' """
            last_key += ' and '+ nth_where
        # print('where '+last_key[5:])
        qry = f"select * from ({tgt_qry}\n union all \n {src_qry}) where {last_key[5:]}"
        df = spark.sql(qry).toPandas().to_dict()
        # print(df)
        for k, v in df.items(): 
            if v[0] != v[1]:
                print(k, v[0] , v[1]) 
                # print(k,"src: "+v[0] , "tgt: "+v[1])#+v[0] , v[1]) 
    
    def t_s_data_completness(self, src_qry, tgt_qry, t_cols, s_cols,bk_cols):
        print("superman")
#         print("Target table is missing rows from source table")
#         # specific_bk_cols = 
#         all_t_s_cols = self.obj.give_bk_cols( t_cols, s_cols,bk_cols)
#         # print(all_t_s_cols)
#         try:
#                 qry = f"""select {self.obj.striper(all_t_s_cols[0])} from ({tgt_qry}) except select {self.obj.striper(all_t_s_cols[1])} from ({src_qry})
#                             """
#                 print(qry)
#                 except_df = spark.sql(qry)
#                 print("these records are missing in src....") #insert record id into final table
#                 # execept_ids  = tuple([row[f'{self.tgt_fst_col}'] for row in except_df.select(f'{self.tgt_fst_col}').collect()])
#                 except_df.display()
# #################### Need to add some future code here ################################
#         except Exception as e:
#             print(e)
        pass
        

    def qry_executor(self,src_qry,tgt_qry, cust,bk_cols):
        # cust = self.obj.customer_check(cust)
        o_src_qry = src_qry
        o_tgt_qry = tgt_qry
        for c in cust:
            src_qry = o_src_qry.format(cust = c)
            tgt_qry = o_tgt_qry.format(cust = c)
            src_df = spark.sql(src_qry)
            tgt_df = spark.sql(tgt_qry)
            # return src_df,tgt_df
            c_src = src_df.count()
            c_tgt = tgt_df.count()
            print(f"Source table has {c_src} records and Target table has {c_tgt} records for cust {c}")
            if c_src == c_tgt :
                self.data_completness(src_qry, tgt_qry,bk_cols)
                pass
            elif c_src > c_tgt:
                self.s_t_data_completness(src_qry, tgt_qry,tgt_df.columns,src_df.columns,bk_cols)
            else :
                self.t_s_data_completness(src_qry, tgt_qry,tgt_df.columns,src_df.columns,bk_cols)

    # def data_validation(self,excep_id,src_qry,tgt_qry):
    #     pass

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
    
class OPS():
    def __init__(self) -> None:
        pass
    
    def ops_1(self, tab_nm, cust, sub_subject):
        obj_qry = DWH_Queries()
        obj_main = Main()
        input_json = obj_qry.data[tab_nm]
        if obj_main.pk_validation(tab_nm,tuple(input_json['pk'].values()) ) == None: return '0'
        if  obj_main.bk_validation(tab_nm,tuple(input_json['bk'].values()) ) == None: return '0'
        # print(input_json['feed'])
        if cust == '*':
            if input_json['feed'] == None :
                cust = input_json['cust_nm']
                src_qry = input_json['src_qry']
                tgt_qry = input_json['tgt_qry']
                print(cust)
                obj_main.qry_executor(src_qry,tgt_qry, cust,tuple(input_json['bk'].values()))
            else :
                # print("lol")
                obj_main.specific_area(input_json['feed'],tuple(input_json['bk'].values()),sub_subject )
                                        
        else : 
            if input_json['sub_subject'] == None :
                obj_main.no_specific_sub_area(input_json,cust)
            else :
                obj_main.specific_area(input_json['feed'],cust)


if __name__ == "__main__":
    tab_nm = "AGT_AGENT_DETAILS_BOLT" 
    cust = '*'
    sub_subject = '*'
    
    obj_ops = OPS()
    obj_ops.ops_1(tab_nm, cust, sub_subject)
