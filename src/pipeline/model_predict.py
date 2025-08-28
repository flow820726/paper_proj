import os
import json
import re
import joblib
import gc
import warnings
import pandas as pd

warnings.filterwarnings('ignore')
gc.collect()

def process_data(dt, mapping_df, encoder_dict=None, logger=None):
    """    
    Process the input DataFrame by encoding categorical variables and handling missing values.
    
    Parameters:
    - dt: DataFrame containing the data to be processed.
    - mapping_df: DataFrame containing the mapping of column names and their variable types.
    - encoder_dict: Dictionary containing label encoders for categorical variables.
    - logger: Logger object for logging information (optional).
    
    Returns:
    - dt: Processed DataFrame with encoded variables.
    - unknown_df: DataFrame containing unknown categorical values that were not found in the encoder classes.
    
    """
    with open("./pipeline/mapping_encoding.json", "r", encoding="utf-8") as f:
        mapping_encoding = json.load(f)    
    original_len = len(dt)
    unknown_records = []

    dt = dt.applymap(lambda x: (-9999) if pd.isna(x) or (isinstance(x, str) and x.strip() == "") else x)
    for _, row in mapping_df.iterrows():
        col = row["col_name"]
        var_type = row["var_type"]

        if var_type == "numeric":
            dt[col] = pd.to_numeric(dt[col], errors="coerce")
        else:
            dt[col] = dt[col].astype("str")
            if col in mapping_encoding:
                col_mapping = mapping_encoding[col]
                for key_class, val_list in col_mapping.items():
                    dt[col] = dt[col].apply(lambda x: key_class if x in val_list else x)
            encoder = encoder_dict[col]
            # unknown cate
            unknown_values = dt[col][~dt[col].isin(encoder.classes_)].unique().tolist()
            for val in unknown_values:
                unknown_records.append({"col": col, "cate": val})
            logger.info(f"{col} not in classes: {unknown_values}")
            dt[col] = dt[col].apply(lambda x: encoder.transform([x])[0] if x in encoder.classes_ else -1)
            dt[col] = dt[col].astype("int32")

    # filter missing data
    encoded_cols = [col for col in encoder_dict.keys() if col not in {"train_test", "event"}]
    if encoded_cols:
        mask = (dt[encoded_cols] == -1).any(axis=1)
        if mask.any():
            dt = dt[~mask].reset_index(drop=True)
        else:
            dt = dt.reset_index(drop=True)

    logger.info(f"process_data: {original_len} → {len(dt)} row(s) retained after encoding")
    unknown_df = pd.DataFrame(unknown_records)

    return dt, unknown_df

def model_predict(folder, logger=None):
    """
    model_predict function
    - folder: save data folder (required)
    - logger: process_record(required)
    """
    
    logger.info(f"Starting model prediction in folder: {folder}")

    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r', encoding='utf-8') as f:
        var_dict = json.load(f)

    model_rule_path = os.path.join(os.path.dirname(__file__), 'model_rule.json')
    with open(model_rule_path, 'r', encoding='utf-8') as f:
        model_rule = json.load(f)

    mapping_list = []
    method_cont = {"occurrence", "weighted_sum"}

    for d, ds_info in var_dict.items():
        vbs = ds_info.get("variables", {})
        for v_name, v_info in vbs.items():
            v_type = v_info.get("var_type", "")
            methods = v_info.get("methods", {})
            for m in methods.keys():
                mapping_type = "numeric" if m in method_cont or v_type == "cont" else "cate"
                col_name = f"{v_name}_{m}"
                mapping_list.append({"col_name": col_name, "var_type": mapping_type})

    mapping_df = pd.DataFrame(mapping_list)

    mask = mapping_df["col_name"].str.startswith("word")
    mapping_df.loc[mask, "col_name"] = (
        mapping_df.loc[mask, "col_name"]
        .str.replace(r'(_)?(suicide|hurt)(_)?', '_', regex=True)
        .str.replace(r'^_+|_+$', '', regex=True)
        .str.replace(r'__+', '_', regex=True)
    )
    mapping_df = mapping_df.drop_duplicates().reset_index(drop=True)

    input_data = f"{folder}/data.csv"
    m_info = model_rule.get("md5", [])
    all_unknowns = []
    for r, r_info in model_rule.items():
        logger.info(f"Processing risk: {r}")
        dt = pd.read_csv(input_data)

        regex_pattern = "(" + "|".join([re.escape(term) for term in m_info]) + ")$"
        mask = mapping_df["col_name"].astype(str).str.contains(regex_pattern, na=False, regex=True)
        mask = mask & (mapping_df["col_name"] != "PS_RDATE_last")
        select_col = mapping_df["col_name"][mask].drop_duplicates().tolist()

        if r in ["suicide_180", "suicide_90"]:
            exclude_term = "hurt"
            rename_col = "suicide"
        elif r in ["hurt_180", "hurt_90"]:
            exclude_term = "suicide"
            rename_col = "hurt"
        else:
            exclude_term = None
            rename_col = None

        if exclude_term:
            drop_mask = dt.columns.str.startswith("word") & dt.columns.str.contains(exclude_term, case=False, na=False)
            dt = dt.loc[:, ~drop_mask]
            rename_dict = {}
            for col in dt.columns:
                if col.startswith("word") and rename_col in col:
                    new_col = re.sub(rf'(_)?{rename_col}(_)?', '_', col)
                    new_col = re.sub(r'^_+|_+$', '', new_col)
                    new_col = re.sub(r'__+', '_', new_col)
                    rename_dict[col] = new_col
            dt = dt.rename(columns=rename_dict)

        mapping_df_new = mapping_df[mapping_df['col_name'].isin(select_col)].reset_index(drop=True)
        encoder_dict = joblib.load(f"./pipeline/models/label_encoders_{r}_md5.pkl")

        dt_tmp = dt[select_col + ['id', 'na_none_na']]
        dt_tmp, unknown_dt = process_data(dt_tmp, mapping_df_new, encoder_dict=encoder_dict, logger=logger)
        unknown_dt["risk"] = r  
        all_unknowns.append(unknown_dt)

        dt_tmp_none_na = dt_tmp[dt_tmp['na_none_na'] == "none_na"].reset_index(drop=True)
        dt_tmp_na = dt_tmp[dt_tmp['na_none_na'] == "na"].reset_index(drop=True)

        total = len(r_info['models'])
        for i, m in enumerate(r_info['models'], 1):
            logger.info(f"[{i}/{total}] Predicting: {m}")
            est = joblib.load(f"./pipeline/models/model_{m}.sav")
            exclude_cols = ['na_none_na', 'id']
            select_col2 = [col for col in select_col if col not in exclude_cols]

            if "na" not in m:
                dt_tmp_overall = dt_tmp[select_col2]
                y_hat = est.predict_proba(dt_tmp_overall)[:, 1]
                dt_tmp[f"overall_{m}"] = y_hat
            elif "none_na" in m:
                col_name = f"step2_variable_{m}"
                dt_tmp_none_na[col_name] = 9999
                if len(dt_tmp_none_na) != 0:
                    dt_tmp_none_na_pred = dt_tmp_none_na[select_col2]
                    y_hat = est.predict_proba(dt_tmp_none_na_pred)[:, 1]
                    dt_tmp_none_na[col_name] = y_hat
            elif "na" in m:
                col_name = f"step2_variable_{m}"
                dt_tmp_na[col_name] = 9999
                if len(dt_tmp_na) != 0:
                    dt_tmp_na_pred = dt_tmp_na[select_col2]
                    y_hat = est.predict_proba(dt_tmp_na_pred)[:, 1]
                    dt_tmp_na[col_name] = y_hat

        dt_na_none_na = pd.concat([dt_tmp_none_na, dt_tmp_na], axis=0, join='outer').fillna(9999).reset_index(drop=True)

        cols_na = ['id'] + [c for c in dt_na_none_na.columns if c.startswith('step2_')]
        dt_na_none_na = dt_na_none_na[cols_na].reset_index(drop=True)

        cols = ['id'] + [c for c in dt_tmp.columns if c.startswith('overall_')]
        dt_overall = dt_tmp[cols].reset_index(drop=True)
        dt_merged = dt_overall.merge(dt_na_none_na, on="id", how="inner").reset_index(drop=True)

        output_file = os.path.join(folder, f"prediction_{r}_md5.csv")
        dt_merged.to_csv(output_file, index=False)
        logger.info(f"[Done] Model predictions have been saved to {output_file}")

    if all_unknowns:
        unknown_all_df = pd.concat(all_unknowns, axis=0).drop_duplicates().reset_index(drop=True)
        unknown_all_df = unknown_all_df[["risk", "col", "cate"]]
        unknown_all_df["cate"] = unknown_all_df["cate"].apply(lambda x: f'"{x}"')

        unknown_output = os.path.join(folder, "unknown_labels.csv")
        unknown_all_df.to_csv(unknown_output, index=False, encoding="utf-8-sig")
        logger.info(f"[Done] Unknown label values saved to: {unknown_output}")

    logger.info("Model prediction completed for all risks.")