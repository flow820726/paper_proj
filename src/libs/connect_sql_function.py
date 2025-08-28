import pandas as pd
import numpy as np
import pyodbc
import warnings
import gc
gc.collect()
warnings.filterwarnings('ignore')

# new
def get_db_name(v, data_base=''):
    
    """
    parmasfro: 
    v(str): table name
    
    將不同資料庫做標準化
    
    "TRAN_PERSON_DATA", "DRUGABUSE"
    
    return 統一的資料庫(str)
    """

    db_map = {
            "past_112":{
                    "CSSMS": ["A_CSSM_REPORT", "A_CSSM_VISIT"],
                    "PsycheCare": ["API_ABSTINENCE_DATA", "API_ICF_DATA", 
                                   "API_MOJ_CCII", "API_MOJ_MJAC", "API_SNNv2CSSM_S1", 
                                   "CCF_MARK_NEW ", "D_DSPC_VISIT_ASSIGN ", "D_DSPC_VISIT_CLOSE", "SWIS_DATA",
                                   "CSSM_DAILY", "D_CSSP_CARE_RSHIP", "D_CSSP_ESCORT", "D_CSSP_VISIT", "D_DSPC_VISIT",
                                   "D_DSPC_VISIT_FAMILY","D_DSPC_VISIT_NEED",
                                   "D_DSPC_VISIT_RISK", "D_ICD_PSY_DATA", "D_NOTICE", "D_PSYCHOSIS", "D_TRANSFER_NORMAL",
                                   "DPSC", "MST_DAILY",  "PENALTY_DATA", "TRAN_PERSON_DATA"], 
                    "formsip" : ["DRCASEINFO", "DRUGABUSE","DRCASETRACK","DRCURETRACK","DRPERSONINFO"], 
                    "Staging_ACTSDB" : ["ACT_Addiction_Case","ACT_Case_Addiction_Substance"]},
            "112":{
                "112formsip":["drcaseinfo_111","drcaseinfo_112", "drcasetrack_111","drpersoninfo_111",
                              "drpersoninfo_112","drtransapply_112"],
                "112PsycheCare":["AC_CASE_M06","API_ABSTINENCE_DATA","API_MOJ_CCII", "API_MOJ_MJAC","API_SNNv2CSSM_S1",
                                 "CCF_MARK_NEW", "CSSM_DAILY", "CSSP_CARE_RSHIP", "CSSP_ESCORT","CSSP_VISIT",
                                 "DSPC_DATA_NEW", "DSPC_VISIT", "DSPC_VISIT_ASSIGN", "DSPC_VISIT_CLOSE", 
                                 "DSPC_VISIT_FAMILY","DSPC_VISIT_NEED","DSPC_VISIT_RISK","ICD_PSY_DATA","MST_DAILY",
                                 "NOTICE", "PENALTY_DATA","PSYCHOSIS","SWIS_DATA","TRAN_PERSON_DATA",
                                 "TRANSFER_NORMAL","VW_API_ICF_DATA", "個案紀錄表","訪視單"]},
            "post_112":{
                "112MentalCareData": ["CSSM_REPORT", "CSSM_VISIT"],
                "MISPCDB": ["API_ABSTINENCE_DATA","API_ICF_DATA","API_MOJ_CCII", "API_MOJ_MJAC",
                            "CCF_MARK_NEW", "CSSM_DAIL", "CSSP_CARE_RSHIP", "CSSP_ESCORT","CSSP_VISIT","CSSP_VISIT_2020_2023",
                            "DSPC_DATA_NEW", "DSPC_VISIT", "DSPC_VISIT_2020_2023", "DSPC_VISIT_ASSIGN", "DSPC_VISIT_CLOSE", 
                            "DSPC_VISIT_FAMILY","DSPC_VISIT_NEED","DSPC_VISIT_RISK","ICD_PSY_DATA","MST_DAILY","NOTICE",
                            "PENALTY_DATA","PSYCHOSIS","SWIS_DATA","TRAN_PERSON_DATA","TRANSFER_NORMAL","VM_API_ICF_DATA"],
                "Staning_精神照護系統語意資料庫":["個案紀錄表","訪視單"]}
        }

    for i, j in db_map[data_base].items():
        if v in j:
            n = i
    
    return(n)

def get_sql_data(table_name, data_base='', col_name='all', cond=''):
    '''
    params:
    col_name (list): 哪些欄位名稱
    cond: query 條件式
    
    return: data frame
    '''
    
    # create query
    if col_name != 'all':
        c_name = ','.join(col_name) if len(col_name) > 1 else col_name[0]
    else:
        c_name = '*'
    
    query = f"SELECT {c_name} FROM {table_name}"
    query += f" WHERE {cond}" if cond else ""
    query += ";"
    
    # get db name
    db_name = get_db_name(table_name, data_base)
    
    # connect sql
    conn_str = (
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=203.65.96.54;"
        "DATABASE=" + db_name + ";"
        "UID=iisiuser;"
        "PWD=iis1@Admin;"
    )
    
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # run query
    cursor.execute(query)
    columns = [column[0] for column in cursor.description]  # 取得欄位名稱
    data = cursor.fetchall()
    conn.close()
    
    # convert to DataFrame
    df = pd.DataFrame.from_records(data, columns=columns)
    
    return df


def get_data(table_name, data_base='', col_name=[], cond='', id_col='', date_col=''):
    '''
    step 1. check key
        1. 欄位 : coln + key_dict[[table_name]]
        2. 去重複

    step 2. get data
        get_sql_data()

    step 3, table_name in names(merge_dict) 

    step 4. 串接main
        1. 取得主檔 : merge_dict[[table_name]][['main_table']]
        2. get_sql_data(主檔, coln=key_dict[[主檔]])
        3. merge by merge_dict[[table_name]][['merge_col']]

    step 4. 修改日期
    '''
    key_dict = {
        "past_112":{
            # CSSM
            "A_CSSM_REPORT" : ['SID','SUICIDEDATE'],    
            "A_CSSM_VISIT" : ['SID','VISITDATE'],
            # Staging_ACTSDB
            "ACT_Addiction_Case" : ['Acts_Sn', 'Apply_Date'], 
            "ACT_Addiction_Substance" : [], 
            "ACT_Case_Addiction_Substance" : ['Acts_Sn'],    
            # Staging_formsip
            "DRCASEINFO" : ['PID', 'serviceDate'],     
            "DRUGABUSE" : ['PID','CRT_DATE'],     
            "DRCASETRACK" : ['PID', 'trackDate'],     
            "DRCURETRACK" : ['PID','trackDate'],   
            "DRPERSONINFO" : ["PID", "Upd_Date"],

            # Staging_PsycheCare
            "D_PSYCHOSIS" : ['PS_PID','PS_RDATE','PS_BIRTHDAY'],     
            "D_CSSP_VISIT" : ['V_V1SEQ', 'V_PID', 'V_TDATE'],     
            "D_CSSP_CARE_RSHIP" : ['CR_PID','V_V1SEQ'],     
            "D_CSSP_ESCORT" : ['ES_PID', 'ES_HDATE'],   
            "D_DSPC_VISIT" : ["_PS_PID_NOUSE", "V_DATE", "ID"],     
            "D_DSPC_VISIT_FAMILY" : ["VISIT_ID","_PS_PID_NOUSE"],     
            "D_DSPC_VISIT_NEED" : ["VISIT_ID", "_PS_PID_NOUSE"],    
            "D_DSPC_VISIT_RISK" : ["VISIT_ID", "_PS_PID_NOUSE"],     
            "D_TRANSFER_NORMAL" : ["IDN","NOTICE_TIME"],     
            "D_NOTICE" : ["IDN","NOTICE_TIME"],     
            "D_ICD_PSY_DATA" : ['PS_PID','INSERTTIME'],    
            "TRAN_PERSON_DATA" : ['pid','servicedate'],
            "DPSC" : ['PS_PID','STARTDATE'],
            "MST_DAILY" : ['SID','AcceptCaseDate'],    
            "API_ABSTINENCE_DATA" : ['PID','AcceptCaseDate'],
            "CSSM_DAILY" : ['SID','REPORTDATE'],    
            "PENALTY_DATA" : ['PID','CAPDATE'],    
            "API_MOJ_MJAC" : ['NAM_IDNO','NAM_MVDT'],    
            "API_MOJ_CCII" : ['CASE_PID','EF_JUDT'],    
            "API_ICF_DATA" : ['ID','APPRAISAL_DATE'],    
            "API_SNNv2CSSM_S1" : ['SID','REPORTDATE','CSSM_DATE'],
            
            "CCF_MARK_NEW ": ['PID', 'UPDTIME'],
            "D_DSPC_VISIT_ASSIGN ":['PS_PID','BEG_DATE'],
            "D_DSPC_VISIT_CLOSE":['_PS_PID_NOUSE','BEG_DATE'],
            "SWIS_DATA":['ID','STARTYM']
        },
        "112":{            # Staging_PsycheCare
            "PSYCHOSIS" : ['PS_PID','PS_RDATE','PS_BIRTHDAY'],     
            "CSSP_VISIT" : ['V_PID','V_V1SEQ', 'V_TDATE'],     # 要再確認單號
            "CSSP_CARE_RSHIP" : ['CR_PID','V_V1SEQ'],
            "CSSP_ESCORT" : ['ES_PID', 'ES_HDATE'],
            "DSPC_VISIT" : ["_VID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID", "V_DATE"],
            "DSPC_VISIT_2020_2023" : ["ID", "V_DATE"],
            "DSPC_VISIT_FAMILY" : ["_DSPC_VISIT_ID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"],     
            "DSPC_VISIT_NEED" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE","ASSIGN_ID"],    
            "DSPC_VISIT_RISK" : ["_DSPC_VISIT_ID_NOUSE", "_PS_PID_NOUSE","ASSIGN_ID"],     
            "TRANSFER_NORMAL" : ["IDN","NOTICE_TIME"],     
            "NOTICE" : ["IDN","NOTICE_TIME"],     
            "ICD_PSY_DATA" : ['PS_PID','INSERTTIME'],    
            "TRAN_PERSON_DATA" : ['pid','servicedate'],
            "DSPC_DATA_NEW" : ['PS_PID','STARTDATE'],
            "MST_DAILY" : ['SID','AcceptCaseDate'],    
            "API_ABSTINENCE_DATA" : ['PID','AcceptCaseDate'],
            "CSSM_DAILY" : ['SID','REPORTDATE'],    
            "PENALTY_DATA" : ['PID','CAPDATE'],    
            "API_MOJ_MJAC" : ['NAM_IDNO','NAM_MVDT'],    
            "API_MOJ_CCII" : ['CASE_PID','EF_JUDT'],    
            "VW_API_ICF_DATA" : ['ID','APPRAISAL_DATE'],    # HSPT_ID to ID
            "API_SNNv2CSSM_S1" : ['SID','REPORTDATE','CSSM_DATE'],
            # Staging_formsip
            "drcaseinfo_111" : ['PID', 'serviceDate'],     
            "drcaseinfo_112" : ['PID', 'serviceDate'],     
            "drcasetrack_111" : ['PID', 'trackDate'],     
            "drtransapply_112" : ['PID', "SERVICEDATE"],           
            "drpersoninfo_111" : ["PID", "Upd_Date"],
            "drpersoninfo_112" : ["PID", "Upd_Date"],            
            "CCF_MARK_NEW": ['PID', 'UPDTIME'],
            "DSPC_VISIT_ASSIGN":['PS_PID','BEG_DATE'],
            "DSPC_VISIT_CLOSE":['_PS_PID_NOUSE','BEG_DATE'],
            "SWIS_DATA":['ID','STARTYM']
        },
        "post_112":{
            "CCF_MARK_NEW": ['PID', 'UPDTIME'],
            "DSPC_VISIT_ASSIGN":['PS_PID','BEG_DATE'],
            "DSPC_VISIT_CLOSE":['_PS_PID_NOUSE','BEG_DATE'],
            "SWIS_DATA":['ID','STARTYM'],
            # CSSM
            "CSSM_REPORT" : ['SID','SUICIDEDATE'],    
            "CSSM_VISIT" : ['SID','VISITDATE'],    
            # Staging_ACTSDB
            "ACT_Addiction_Case" : ['Acts_Sn', 'Apply_Date'], 
            "ACT_Addiction_Substance" : [], 
            "ACT_Case_Addiction_Substance" : ['Acts_Sn'],    
            # Staging_PsycheCare
            "PSYCHOSIS" : ['PS_PID','PS_RDATE','PS_BIRTHDAY'],     
            "CSSP_VISIT" : ['V_PID','V_V1SEQ', 'V_TDATE'],     # 要再確認單號
            "CSSP_VISIT_2020_2023" : ['V_V1SEQ', 'V_TDATE'],     
            "CSSP_CARE_RSHIP" : ['CR_PID','V_V1SEQ'],
            "CSSP_ESCORT" : ['ES_PID', 'ES_HDATE'],
            "DSPC_VISIT" : ["ID", "V_DATE"],
            "DSPC_VISIT_2020_2023" : ["ID", "V_DATE"],
            "DSPC_VISIT_FAMILY" : ["VISIT_ID","_PS_PID_NOUSE"],     
            "DSPC_VISIT_NEED" : ["VISIT_ID", "_PS_PID_NOUSE"],    
            "DSPC_VISIT_RISK" : ["VISIT_ID", "_PS_PID_NOUSE"],     
            "TRANSFER_NORMAL" : ["IDN","NOTICE_TIME"],     
            "NOTICE" : ["IDN","NOTICE_TIME"],     
            "ICD_PSY_DATA" : ['PS_PID','INSERTTIME'],    
            "TRAN_PERSON_DATA" : ['pid','servicedate'],
            "DSPC_DATA_NEW" : ['PS_PID','START_DATE'],
            "MST_DAILY" : ['SID','AcceptCaseDate'],    
            "API_ABSTINENCE_DATA" : ['PID','AcceptCaseDate'],
            "CSSM_DAIL" : ['SID','REPORTDATE'],    
            "PENALTY_DATA" : ['PID','CAPDATE'],    
            "API_MOJ_MJAC" : ['NAM_IDNO','NAM_MVDT'],    
            "API_MOJ_CCII" : ['CASE_PID','EF_JUDT'],    
            "API_ICF_DATA" : ['ID','APPRAISAL_DATE'],    # HSPT_ID to ID
            "VM_API_ICF_DATA" : ['ID','APPRAISAL_DATE'],    # HSPT_ID to ID
            "訪視單":["V_PID","V_V1SEQ"]}
        }        
    sub_table_dict = {
        "past_112": {        
            "ACT_Case_Addiction_Substance" : {
                "main_table" : "ACT_Addiction_Case",
                "merge_col_main" : ["Acts_Sn"],
                "merge_col_sub" : ["Acts_Sn"]},
            "D_CSSP_CARE_RSHIP" : {
                "main_table" : "D_CSSP_VISIT",
                "merge_col_main" : ["V_PID","V_V1SEQ"],
                "merge_col_sub" : ["CR_PID","V_V1SEQ"]},
            "D_DSPC_VISIT_FAMILY" : {
                "main_table" : "D_DSPC_VISIT",
                "merge_col_main" : ["ID", "_PS_PID_NOUSE"],
                "merge_col_sub" : ["VISIT_ID", "_PS_PID_NOUSE"]},
            "D_DSPC_VISIT_NEED" : {
                "main_table" : "D_DSPC_VISIT",
                "merge_col_main" : ["ID", "_PS_PID_NOUSE"],
                "merge_col_sub" : ["VISIT_ID", "_PS_PID_NOUSE"]},
            "D_DSPC_VISIT_RISK" : {
                "main_table" : "D_DSPC_VISIT",
                "merge_col_main" : ["ID", "_PS_PID_NOUSE"],
                "merge_col_sub" : ["VISIT_ID", "_PS_PID_NOUSE"]}},
        "112":{
            "ACT_Case_Addiction_Substance" : {
                "main_table" : "ACT_Addiction_Case",
                "merge_col_main" : ["Acts_Sn"],
                "merge_col_sub" : ["Acts_Sn"]},
            "CSSP_CARE_RSHIP" : {
                "main_table" : "CSSP_VISIT",
                "merge_col_main" : ['V_PID',"V_V1SEQ"],
                "merge_col_sub" : ["CR_PID","V_V1SEQ"]},
            "DSPC_VISIT_FAMILY" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["_VID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"],
                "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"]},
            "DSPC_VISIT_NEED" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["_VID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"],
                "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"]},
            "DSPC_VISIT_RISK" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["_VID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"],
                "merge_col_sub" : ["_DSPC_VISIT_ID_NOUSE","_PS_PID_NOUSE","ASSIGN_ID"]},
            "訪視單" : {
                "main_table" : "CSSP_VISIT_2020_2023",
                "merge_col_main" : ["V_V1SEQ"],
                "merge_col_sub" : ["V_V1SEQ"]}
        },
        "post_112":{
            "ACT_Case_Addiction_Substance" : {
                "main_table" : "ACT_Addiction_Case",
                "merge_col_main" : ["Acts_Sn"],
                "merge_col_sub" : ["Acts_Sn"]},
            "CSSP_CARE_RSHIP" : {
                "main_table" : "CSSP_VISIT",
                "merge_col_main" : ['V_PID',"V_V1SEQ"],
                "merge_col_sub" : ["CR_PID","V_V1SEQ"]},
            "DSPC_VISIT_FAMILY" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["ID"],
                "merge_col_sub" : ["VISIT_ID"]},
            "DSPC_VISIT_NEED" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["ID"],
                "merge_col_sub" : ["VISIT_ID"]},
            "DSPC_VISIT_RISK" : {
                "main_table" : "DSPC_VISIT",
                "merge_col_main" : ["ID"],
                "merge_col_sub" : ["VISIT_ID"]},
            "訪視單" : {
                "main_table" : "CSSP_VISIT_2020_2023",
                "merge_col_main" : ["V_V1SEQ"],
                "merge_col_sub" : ["V_V1SEQ"]}
        }}
    rename_table_list = {"past_112":["D_DSPC_VISIT_FAMILY","D_DSPC_VISIT_NEED","D_DSPC_VISIT_RISK"],
                         "112":["DSPC_VISIT_FAMILY","DSPC_VISIT_NEED","DSPC_VISIT_RISK"],
                         "post_112":["DSPC_VISIT_FAMILY","DSPC_VISIT_NEED","DSPC_VISIT_RISK"]}
    
    date_table_list = {"past_112":['PENALTY_DATA','API_ICF_DATA', 'API_MOJ_MJAC', 'API_MOJ_CCII', 'API_ICF_SNNv2CSSM_S1'],
                       "112":['PENALTY_DATA','VW_API_ICF_DATA', 'API_MOJ_MJAC', 'API_MOJ_CCII', 'API_ICF_SNNv2CSSM_S1'],
                       "post_112":['PENALTY_DATA','API_ICF_DATA', 'API_MOJ_MJAC', 'API_MOJ_CCII', 'API_ICF_SNNv2CSSM_S1', 'VM_API_ICF_DATA']}
    
    date_table_list2 = {"past_112":['SWIS_DATA'],
                        "112":['SWIS_DATA'],
                        "post_112":['SWIS_DATA']}
    
    key_dict = key_dict[data_base]
    sub_table_dict = sub_table_dict[data_base]
    rename_table_list = rename_table_list[data_base]
    date_table_list = date_table_list[data_base]
    date_table_list2 = date_table_list2[data_base]

    col_name = [x for x in col_name if x not in key_dict[table_name]]
    col_name = col_name + key_dict[table_name]
    
    if col_name == []: 
        df = get_sql_data(table_name = table_name,
                          data_base = data_base,
                          cond = cond)        
    else:
        df = get_sql_data(table_name = table_name,
                          data_base = data_base,
                          col_name = col_name,
                          cond = cond)

    if(table_name in list(sub_table_dict.keys())):
        if(table_name in rename_table_list):
            df[sub_table_dict[table_name]["merge_col_sub"]] = df[sub_table_dict[table_name]["merge_col_sub"]].astype(str)

            # rename
            date_df = get_sql_data(table_name=sub_table_dict[table_name]['main_table'],
                                   data_base = data_base,
                                   col_name=key_dict[sub_table_dict[table_name]['main_table']])
            
            date_df[sub_table_dict[table_name]["merge_col_main"]] = date_df[sub_table_dict[table_name]["merge_col_main"]].astype(str)
            
            df = df.merge(date_df,
                          left_on=sub_table_dict[table_name]["merge_col_sub"],
                          right_on=sub_table_dict[table_name]["merge_col_main"],
                          how='inner')             
        else:
            df[sub_table_dict[table_name]["merge_col_sub"]] = df[sub_table_dict[table_name]["merge_col_sub"]].astype(str)

            date_df = get_sql_data(table_name=sub_table_dict[table_name]['main_table'],
                                   data_base = data_base,
                                   col_name=key_dict[sub_table_dict[table_name]['main_table']])
            date_df[sub_table_dict[table_name]["merge_col_main"]] = date_df[sub_table_dict[table_name]["merge_col_main"]].astype(str)

            df = df.merge(date_df,
                          left_on=sub_table_dict[table_name]["merge_col_sub"],
                          right_on=sub_table_dict[table_name]["merge_col_main"],
                          how='inner')  
        
    # clean id col
    if id_col != '':
        df[id_col] = df[id_col].str.replace('Encrypted', '')
        df[id_col] = df[id_col].str.replace('-', '')
    
    # change to datetime
    if date_col != '':
        if table_name in date_table_list:
            for i in date_col:
                df['new_date'] = pd.to_numeric(df[i], errors='coerce') + 19110000
                df['new_date'] = np.where((df['new_date'] < 19110000) | (df['new_date'] > 20300000), np.nan, df['new_date'])
                df['new_date'] = df['new_date'].apply(str)
                df['new_date'] = df['new_date'].str.slice(0, 8)
                df[i] = pd.to_datetime(df['new_date'], errors='coerce').dt.normalize() #統一格式
                df = df.drop(['new_date'], axis=1)
        elif table_name in date_table_list2:
            for i in date_col:
                df['new_date'] = pd.to_numeric(df[i], errors='coerce')*100 + 19110001
                df['new_date'] = np.where((df['new_date'] < 19110000) | (df['new_date'] > 20300000), np.nan, df['new_date'])
                df['new_date'] = df['new_date'].apply(str)
                df['new_date'] = df['new_date'].str.slice(0, 8)
                df[i] = pd.to_datetime(df['new_date'], errors='coerce').dt.normalize() #統一格式
                df = df.drop(['new_date'], axis=1)
        else:
            for i in date_col:
                df[i] = pd.to_datetime(df[i], errors="coerce").dt.normalize() #統一格式
                
    return df

def merge_tables(table_name, col_name, cond, id_col, date_col):
    merge_dict = {
        "CSSM_REPORT" : {
                "db" : {
                    "past_112" : "A_CSSM_REPORT",
                    "post_112" : "CSSM_REPORT"
                    },
                "diff_cols":{

            }
        },
        "CSSM_VISIT" : {
                "db" : {
                    "past_112" : "A_CSSM_VISIT",
                    "post_112" : "CSSM_VISIT"
                    },
                "diff_cols":{

            }
        },

        "PSYCHOSIS" : {
            "db" : {
                "past_112" : "D_PSYCHOSIS",
                "112" : "PSYCHOSIS",
                "post_112" : "PSYCHOSIS"
            },
            "diff_cols":{

            }
        },

        "CSSP_VISIT" : {
            "db" : {
                "past_112" : "D_CSSP_VISIT",
                "112" : "CSSP_VISIT",
                "post_112" : ["CSSP_VISIT", "CSSP_VISIT_2020_2023"]
            },
            "diff_cols":{

            }
        },

        "CSSP_CARE_RSHIP" : {
            "db" : {
                "past_112" : "D_CSSP_CARE_RSHIP",
                "112" : "CSSP_CARE_RSHIP",
                "post_112" : "CSSP_CARE_RSHIP"
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT",
                "112" : "DSPC_VISIT",
                "post_112" : ["DSPC_VISIT", "DSPC_VISIT_2020_2023"]
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT_FAMILY" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT_FAMILY",
                "112" : "DSPC_VISIT_FAMILY",
                "post_112" : "DSPC_VISIT_FAMILY"
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT_RISK" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT_RISK",
                "112" : "DSPC_VISIT_RISK",
                "post_112" : "DSPC_VISIT_RISK"
            },
            "diff_cols":{

            }
        },

        "TRANSFER_NORMAL" : {
            "db" : {
                "past_112" : "D_TRANSFER_NORMAL",
                "112" : "TRANSFER_NORMAL",                
                "post_112" : "TRANSFER_NORMAL"
            },
            "diff_cols":{

            }
        },

        "NOTICE" : {
            "db" : {
                "past_112" : "D_NOTICE",
                "112" : "NOTICE",
                "post_112" : "NOTICE"
            },
            "diff_cols":{

            }
        },

        "CSSP_ESCORT" : {
            "db" : {
                "past_112" : "D_CSSP_ESCORT",
                "112" : "CSSP_ESCORT",
                "post_112" : "CSSP_ESCORT"
            },
            "diff_cols":{

            }
        },

        "ICD_PSY_DATA" : {
            "db" : {
                "past_112" : "D_ICD_PSY_DATA",
                "112" : "ICD_PSY_DATA",
                "post_112" : "ICD_PSY_DATA"
            },
            "diff_cols":{

            }
        },

        "TRAN_PERSON_DATA" : {
            "db" : {
                "past_112" : "TRAN_PERSON_DATA",
                "112" : "TRAN_PERSON_DATA",
                "post_112" : "TRAN_PERSON_DATA"
            },
            "diff_cols":{

            }
        },

        "DPSC" : {
            "db" : {
                "past_112" : "DPSC",
                "112" : "DSPC_DATA_NEW",
            },
            "diff_cols":{

            }
        },
        "DSPC_DATA_NEW" : {
            "db" : {
                "post_112" : "DSPC_DATA_NEW"
            },
            "diff_cols":{

            }
        },

        "MST_DAILY" : {
            "db" : {
                "past_112" : "MST_DAILY",
                "112" : "MST_DAILY",
                "post_112" : "MST_DAILY"
            },
            "diff_cols":{

            }
        },

        "API_ABSTINENCE_DATA" : {
            "db" : {
                "past_112" : "API_ABSTINENCE_DATA",
                "112" : "API_ABSTINENCE_DATA",
                "post_112" : "API_ABSTINENCE_DATA"
            },
            "diff_cols":{

            }
        },

        "CSSM_DAILY" : {
            "db" : {
                "past_112" : "CSSM_DAILY",
                "112" : "CSSM_DAILY",
                "post_112" : "CSSM_DAIL"
            },
            "diff_cols":{

            }
        },

        "PENALTY_DATA" : {
            "db" : {
                "past_112" : "PENALTY_DATA",
                "112" : "PENALTY_DATA",
                "post_112" : "PENALTY_DATA"
            },
            "diff_cols":{

            }
        },

        "API_MOJ_MJAC" : {
            "db" : {
                "past_112" : "API_MOJ_MJAC",
                "112" : "API_MOJ_MJAC",
                "post_112" : "API_MOJ_MJAC"
            },
            "diff_cols":{

            }
        },

        "API_MOJ_CCII" : {
            "db" : {
                "past_112" : "API_MOJ_CCII",
                "112" : "API_MOJ_CCII",
                "post_112" : "API_MOJ_CCII"
            },
            "diff_cols":{

            }
        },

        "API_ICF_DATA" : {
            "db" : {
                "past_112" : "API_ICF_DATA",
                "112" : "VW_API_ICF_DATA",
                "post_112" : ["API_ICF_DATA", "VM_API_ICF_DATA"]
            },
            "diff_cols":{

            }
        },

        "API_SNNv2CSSM_S1" : {
            "db" : {
                "past_112" : "API_SNNv2CSSM_S1",
                "112" : "API_SNNv2CSSM_S1",
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT_NEED" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT_NEED",
                "112" : "DSPC_VISIT_NEED",
                "post_112" : "DSPC_VISIT_NEED"
            },
            "diff_cols":{

            }
        },

        "DRCASEINFO" : {
            "db" : {
                "past_112" : "DRCASEINFO",
                "112" : ["drcaseinfo_111", "drcaseinfo_112"]
            },
            "diff_cols":{

            }
        },

        "DRUGABUSE" : {
            "db" : {
                "past_112" : "DRUGABUSE"
            },
            "diff_cols":{

            }
        },

        "DRCASETRACK" : {
            "db" : {
                "past_112" : "DRCASETRACK",
                "112" : "drcasetrack_111"
            },
            "diff_cols":{

            }
        },

        "DRCURETRACK" : {
            "db" : {
                "past_112" : "DRCURETRACK"
            },
            "diff_cols":{

            }
        },
        "DRPERSONINFO" : {
            "db" : {
                "past_112" : "DRPERSONINFO",
            },
            "diff_cols":{

            }
        },
        "DRTRANSAPPLY" : {
            "db" : {
                "112" : "drtransapply_112"
            },
            "diff_cols":{

            }
        },
        "ACT_Addiction_Case" : {
            "db" : {
                "past_112" : "ACT_Addiction_Case"
            },
            "diff_cols":{

            }
        },

        "ACT_Case_Addiction_Substance" : {
            "db" : {
                "past_112" : "ACT_Case_Addiction_Substance"
            },
            "diff_cols":{

            }
        },

        "CCF_MARK_NEW" : {
            "db" : {
                "past_112" : "CCF_MARK_NEW ", # new
                "112" : "CCF_MARK_NEW",
                "post_112" : "CCF_MARK_NEW"
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT_ASSIGN" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT_ASSIGN ", # new
                "112" : "DSPC_VISIT_ASSIGN",
                "post_112" : "DSPC_VISIT_ASSIGN"
            },
            "diff_cols":{

            }
        },

        "DSPC_VISIT_CLOSE" : {
            "db" : {
                "past_112" : "D_DSPC_VISIT_CLOSE", # new                
                "112" : "DSPC_VISIT_CLOSE",
                "post_112" : "DSPC_VISIT_CLOSE"
            },
            "diff_cols":{

            }
        },
        "SWIS_DATA" : {
            "db" : {
                "past_112" : "SWIS_DATA", # new                
                "112" : "SWIS_DATA",
                "post_112" : "SWIS_DATA"
            },
            "diff_cols":{

            }
        },
        "訪視單" : {
            "db" : {
                "post_112" : "訪視單"
            },
            "diff_cols":{

            }
        }
    }
    
    df_combine = pd.DataFrame()
    db_mapping = merge_dict[table_name]["db"]
    for db_name, tb_names in db_mapping.items():
        if isinstance(tb_names,str):
            tb_names = [tb_names]
        for tb_name in tb_names:    
            dt_tmp = get_data(tb_name, db_name, col_name, cond, id_col, date_col)
            # rename col
            rename_dict = {}
            for c, col_versions in merge_dict[table_name]["diff_cols"].items():
                if db_name in col_versions:
                    rename_dict[col_versions[db_name]] = c
            if rename_dict:
                dt_tmp.rename(columns = rename_dict, inplace = True)
            df_combine = pd.concat([df_combine,dt_tmp], axis=0, ignore_index=True)
            df_combine = df_combine.drop_duplicates() 

    # 去重複
    df_combine = df_combine.drop_duplicates() 
    df_combine = df_combine.reset_index(drop=True)
    return(df_combine)
