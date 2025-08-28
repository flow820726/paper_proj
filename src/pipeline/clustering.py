import pandas as pd
import gc
import warnings
import os
import json

warnings.filterwarnings('ignore')
gc.collect()

def clustering_main(folder, logger=None, time=None):
    """
    clustering_main function
    - folder: save data folder (required)
    - logger: process_record (required)
    """
    
    model_rule_path = os.path.join(os.path.dirname(__file__), 'risk_rule_dict_0612.json')
    with open(model_rule_path, 'r', encoding='utf-8') as f:
        model_rule = json.load(f)

    for c, c_rule in model_rule.items():
        # input_path = os.path.join(folder, f"prediction_{c}_md5.csv")
        input_path = os.path.join(folder, f"prediction_{c}_{time}.csv")
        
        if not os.path.exists(input_path):
            logger.warning(f"[error] no data: {input_path}")
            continue

        dt = pd.read_csv(input_path)
        dt["risk"] = "medium"

        # high risk
        high_conditions = c_rule.get("high", [])
        if high_conditions:
            high_query = " | ".join(f"({cond})" for cond in high_conditions)
            try:
                dt.loc[dt.query(high_query).index, "risk"] = "high"
            except Exception as e:
                logger.error(f"[error] failed：{e}\nerror message：{high_query}")

        # low risk
        low_conditions = c_rule.get("low", [])
        if low_conditions:
            low_query = " | ".join(f"({cond})" for cond in low_conditions)
            try:
                low_index = dt.query(low_query).index
                dt.loc[(dt["risk"] == "medium") & (dt.index.isin(low_index)), "risk"] = "low"
            except Exception as e:
                logger.error(f"[error] failed：{e}\nerror message：{low_query}")

        # output result
        dt_result = dt[["id", "risk",'event_90','event_180']]
        output_path = os.path.join(folder, f"risk_level_{c}.csv")
        dt_result.to_csv(output_path, index=False)
        logger.info(f"[High-Risk List Completed] Saved to {output_path}")
