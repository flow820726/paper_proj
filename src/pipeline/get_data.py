import os
import json
import pandas as pd
import numpy as np
from libs.connect_sql_function import merge_tables
from libs.get_data_function import get_variable
from libs.connect_sql_function import get_data

def get_data_main(folder, time=None, logger=None):
    """
    get_data_main function
    - folder: save data folder (required)
    - time: external specified time (required)
    - logger: process_record (required)
    """
    logger.info(f"Starting get_data... Data will be saved to: {folder}")

    # Step 1: read config.json
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        var_dict = json.load(f)

    # Step 2: get data
    logger.info("Fetching data from database...")
    dt_basic = merge_tables(
        table_name="PSYCHOSIS",
        col_name=['PS_PID', 'PS_BIRTHDAY', 'PS_RDATE'],
        cond='',
        id_col='PS_PID',
        date_col=['PS_RDATE']
    )

    # Step 3: basic cleaning
    dt_basic = dt_basic.replace(
        to_replace=r"(?i)^(nan|null|none|\s*)$",
        value=np.nan,
        regex=True
    )
    dt_basic['index_date'] = pd.to_datetime(time)

    dt_basic.rename(columns={'PS_PID': 'id'}, inplace=True)
    dt_basic = dt_basic[['id', 'index_date']]
    dt_basic = dt_basic.drop_duplicates()
    dt_basic = get_variable(dt_basic, var_dict)

    # Step 4: create na_none_na column
    selected_keys = ["DSPC_VISIT_FAMILY", "DSPC_VISIT_NEED", "DSPC_VISIT_RISK"]
    variable_names = []
    for key in selected_keys:
        variables = var_dict.get(key, {}).get("variables", {})
        variable_names.extend(variables.keys())

    pattern = "|".join(variable_names)
    exclude_columns = dt_basic.columns[dt_basic.columns.str.contains(pattern, case=False, regex=True)]

    if len(exclude_columns) > 0:
        dt_basic["na_none_na"] = np.where(
            dt_basic[exclude_columns].isin(["9999", 9999, "9999.0", 9999.0]).any(axis=1),
            "na", "none_na"
        )
    else:
        dt_basic["na_none_na"] = "none_na"

    # Step 5: filter data
    original_rows = dt_basic.shape[0]

    # 5.1 filter by sex and age
    dt_basic = dt_basic[((dt_basic['SEX_last'] == "1") | (dt_basic['SEX_last'] == "2"))] 
    logger.info(f"sex filter, leave {dt_basic.shape[0]}")
    dt_basic["AGE_n_last"] = pd.to_numeric(dt_basic["AGE_n_last"], errors="coerce")
    dt_basic = dt_basic[(dt_basic["AGE_n_last"] > 0) & (dt_basic["AGE_n_last"] < 100)]
    logger.info(f"age filter, leave {dt_basic.shape[0]} ")

    # 5.2 filter by id
    id_pattern = r"^[A-Z][12]\d{8}$"
    dt_basic = dt_basic[dt_basic['id'].str.match(id_pattern, na=False)] 
    logger.info(f"id filter, leave {dt_basic.shape[0]} ")

    # 5.3 filter by date
    dt_basic["days_diff"] = (dt_basic["index_date"] - dt_basic["PS_RDATE_last"]).dt.days 
    dt_basic = dt_basic[dt_basic["days_diff"] > 365]
    logger.info(f"followup filter, leave {dt_basic.shape[0]} ")

    # 5.4 filter by death
    dt_death = get_data(table_name="PSYCHOSIS",
                        data_base="post_112",
                        col_name=['PS_PID', 'PS_RDATE','DEAD','DELTIME'],
                        cond='',
                        id_col='PS_PID',
                        date_col=['PS_RDATE'])
    dt_death.rename(columns={'PS_PID': 'id'}, inplace=True)
    dt_death = dt_death.replace(to_replace=r"(?i)^(nan|null|none|\s*)$",
                                value=np.nan,
                                regex=True
                                )
    dt_death = dt_death.dropna(subset=['PS_RDATE'])
    dt_death = dt_death.loc[dt_death.groupby('id')['PS_RDATE'].idxmax()]
    dt_death = dt_death[['id','DEAD','DELTIME']]
    dt_basic = dt_basic.merge(dt_death, on="id", how="left").reset_index(drop=True)
    dt_basic = dt_basic[((dt_basic['DEAD'].isna()) & (dt_basic['DELTIME'].isna()))]
    logger.info(f"death filter, leave {dt_basic.shape[0]} ")
    logger.info(f"Data filtering completed: from {original_rows} rows → {dt_basic.shape[0]} rows")

    # Step 6: save
    output_path = os.path.join(folder, "data.csv")
    dt_basic.to_csv(output_path, index=False)
    logger.info(f"Data saved to: {output_path}")
    logger.info("get_data finished.")
    return dt_basic
