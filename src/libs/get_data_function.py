import pandas as pd
import warnings
import gc
warnings.filterwarnings('ignore')
gc.collect()

from libs.connect_sql_function import merge_tables

from libs.variable_function import (
    calculate_str_isin,
    calculate_isin,
    calculate_diff_date,
    fetch_last_data,
    fetch_exist_data,
    process_occurrence,
    process_last_weighted,
    process_average,
    process_weighted_average,
    process_std,
    process_regression,
    calculate_weighted_sum
)

cal_functions = {
    "str_isin": calculate_str_isin,
    "isin": calculate_isin,
    "diff_date": calculate_diff_date
}

method_functions = {
    "last": fetch_last_data,
    "id_exist": fetch_exist_data,
    "occurrence": process_occurrence,
    "last_weighted": process_last_weighted,
    "average": process_average,
    "weighted_average": process_weighted_average,
    "std": process_std,
    "regression": process_regression,
    "weighted_sum": calculate_weighted_sum
}

def preprocess_variables(dt, dtid, col_name, date_col, m, params): 
    """
    Preprocess variables based on the specified method and parameters.
    
    Parameters:
    - dt: DataFrame containing the data to be processed.
    - dtid: DataFrame containing the IDs and index dates.
    - col_name: Name of the column to be processed.
    - date_col: Name of the date column in the DataFrame.
    - m: Method to be applied for processing.
    - params: Dictionary containing parameters for the method.
    
    Returns:
    - dtid: Updated DataFrame with processed variables.
        
    """

    # step1: check date type
    dt['index_date'] = pd.to_datetime(dt['index_date'], errors="coerce").dt.normalize()
    dt[date_col] = pd.to_datetime(dt[date_col], errors="coerce").dt.normalize()

    # step2: cal diff
    follow_up = params["follow_up"]
    dt['diff'] = (dt['index_date'] - dt[date_col]).dt.days
    dt = dt[(0 < dt['diff']) & (dt['diff'] < follow_up)]
    dt = dt.sort_values(by='diff')
    
    # calculate weight
    dt['weight'] = (follow_up - dt['diff']) / follow_up
    
    # check na id in range true: 9999 false: -9999
    na_id = list(set(dtid['id']) - set(dt['id']))
    
    method = method_functions[m]
    params["dt"] = dt.copy()
    params["col_name"] = col_name
    dt = method(**params)
    dtid = dtid.merge(dt, on = 'id', how = 'left')

    # fill na
    na_0_col = ["id_exist", "occurrence", "weighted_sum"]
    if m in na_0_col:
        dtid.loc[dtid[col_name].isna(), col_name] = 0
    else:
        isna_col = f"{col_name}_isna"

        # 預設條件都為 False，避免欄位不存在錯誤
        cond_沒選 = cond_沒填 = pd.Series(False, index=dtid.index)

        if isna_col in dtid.columns:
            cond_沒選 = ~dtid[isna_col] & dtid[col_name].isna()
            cond_沒填 = dtid[isna_col] & dtid[col_name].isna()

        # 1. 完全沒出現在原始資料（可能是沒進來做這題）
        dtid.loc[dtid['id'].isin(na_id) & dtid[col_name].isna(), col_name] = "9999"

        # 2. 有出現在資料中，但該欄是空的 → 忘了填
        dtid.loc[~dtid['id'].isin(na_id) & cond_沒填, col_name] = "-9999"

        # 3. 有填寫這題，但沒選中這個選項
        dtid.loc[~dtid['id'].isin(na_id) & cond_沒選, col_name] = "0"

    dt = pd.DataFrame()
    params["dt"] = pd.DataFrame()
    
    del params, dt
    gc.collect()

    return dtid

def get_variable(dt_id, var_dict):
    
    """
    Main function to preprocess variables based on the provided variable dictionary.    
    """
    
    for tb_name, tb_content in var_dict.items():
        id_col = tb_content["common_params"]["id_col"]
        date_col = tb_content["common_params"]["date_col"]
        
        for var_name, table_info in tb_content["variables"].items():
            
            # step1: get_data()
            var_type = table_info['var_type']
            cols = list(set(table_info['columns']))
            c_var = [x for x in cols if x not in ["index_date"]][0]
            
            print(f"Table(col):{tb_name}({var_name})")

            get_data_params = {
                                "table_name" : tb_name, # str
                                "col_name" : [c_var] + [id_col] , # list
                                "cond" : '', # str
                                "id_col" : id_col, # str
                                "date_col" : [date_col] # list
            }
            
            df_var = merge_tables(**get_data_params)
            
            # step2: merge id data
            df_var.rename(columns={id_col:"id"}, inplace = True)
            df_var = df_var.merge(dt_id[['id','index_date']], on = 'id', how = 'inner') 
            
            # data_type transform:
            if ((var_type == "cont") or (var_type == "ord")) and (var_name != "AGE_n"):
                df_var[var_name] = pd.to_numeric(df_var[var_name], errors='coerce').astype('float32')

            # step3: select calculate_method or not
            if table_info["c_m"] != {''}:
                for c, p_c in table_info["c_m"].items():
                    cal_fun = cal_functions[c]
                    p_c["dt"] = df_var.copy()
                    p_c["col_name"] = c_var
                    df_var = cal_fun(**p_c)
                    del p_c
                    gc.collect()
                    
            # step4 / step5: transfrom: todo: params flixible
            for m, params in table_info["methods"].items():
        
                dt_id = preprocess_variables(df_var, dt_id, c_var, date_col, m, params)
                dt_id = dt_id.rename(columns={c_var:f"{var_name}_{m}"})
                print(dt_id[f"{var_name}_{m}"].nunique())
            
            df_var = None
            del df_var
            gc.collect()
            
        print("all preprocess done")

    return dt_id
