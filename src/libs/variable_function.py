import pandas as pd
import numpy as np
import re
import gc

# step3: calculate_functions:
def fillna_multiple(dt, col_name, value = 0):
    """
    Fill NaN values in the specified column with a given value.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to fill NaN values in.
    - value: Value to fill NaN entries with.
    
    Returns:
    - DataFrame with NaN values in the specified column replaced by the given value.
    """
    dt[col_name] = dt[col_name].fillna(value)
    return dt

def calculate_str_isin(dt, col_name, name_list):
    """
    Check if the values in the specified column contain any of the strings in name_list.

    Parameters:
    - dt: DataFrame containing the data.    
    - col_name: Name of the column to check for string matches.
    - name_list: List of strings to check against the column values.

    Returns:
    - DataFrame with the specified column updated to 1 if a match is found, otherwise NaN.
    """
    
    pattern = '|'.join(map(re.escape, name_list))
    # dt = dt[dt[col_name].str.contains(pattern, na=False, regex=True)]
    dt[col_name] = dt[col_name].str.contains(pattern, na=False, regex=True).astype(int)
    dt[col_name] = dt[col_name].replace(0, np.nan)
    return dt

def calculate_isin(dt, col_name, name_list): 

    dt[col_name] = dt[col_name].isin(name_list).astype(int)
    dt[col_name] = dt[col_name].replace(0, np.nan)
    return dt

def calculate_diff_date(dt, col_name, col_name2):
    """
    Calculate the difference in years between two date columns in a DataFrame.
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the first date column.
    - col_name2: Name of the second date column.
    Returns:
    - DataFrame with a new column containing the difference in years between the two date columns.
    """

    dt["tmp"] = None
    dt[col_name] = pd.to_datetime(dt[col_name], errors="coerce").dt.normalize() #統一格式
    dt[col_name2] = pd.to_datetime(dt[col_name2], errors="coerce").dt.normalize() #統一格式
    valid_range = (dt[col_name].dt.year >= 1900) & (dt[col_name].dt.year <= 2025)
    value_idx = (dt[col_name2].notna()) & valid_range
    dt.loc[value_idx,'tmp'] = (dt.loc[value_idx, col_name2] - dt.loc[value_idx, col_name]).dt.days // 365
    dt[col_name] = dt['tmp']
    dt.drop(columns=['tmp'])
    return dt

# step4: method_functions: 
def fetch_last_data(dt, col_name, follow_up):
    """
    Fetch the last non-null value of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to fetch the last non-null value from.
    - follow_up: Not used in this function, but can be included for consistency with other functions.
    
    Returns:
    - DataFrame with 'id' and the last non-null value of the specified column.
    """

    dt = dt.groupby(['id']).last().reset_index()
    return dt[['id',col_name]]

def fetch_exist_data(dt, col_name, follow_up):
    """
    Check if a specified column exists for each unique 'id' in the DataFrame and return a binary indicator.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to check for existence.
    - follow_up: Not used in this function, but can be included for consistency with other functions.
    
    Returns:
    - DataFrame with 'id' and a binary indicator (1 if the column exists, 0 otherwise).
    """

    if dt.empty:
        return pd.DataFrame(columns=['id', col_name])
    all_ids = dt['id'].unique()

    grouped = dt.dropna(subset = [col_name]).groupby('id')[col_name].count()
    result = grouped.gt(0).astype(int).reset_index(name = col_name)
    result = result.set_index('id').reindex(all_ids, fill_value=0).reset_index()
    return result

def process_occurrence(dt, col_name, follow_up):    
    """
    Process the occurrence of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to process occurrences for.
    - follow_up: Not used in this function, but can be included for consistency with other

    Returns:
    - DataFrame with 'id' and the count of occurrences of the specified column.
    """

    dt = (dt.groupby('id')[col_name].count().reset_index(name=col_name))
    
    return dt

def process_last_weighted(dt, col_name, follow_up):
    """
    Process the last weighted value of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to process the last weighted value for.
    - follow_up: Not used in this function, but can be included for consistency with other functions.
    
    Returns:
    - DataFrame with 'id' and the last weighted value of the specified column.  

    """

    dt['weighted_value'] = dt['weight'] * dt[col_name]
    dt = dt.groupby('id').last().reset_index()
    dt = dt[['id', 'weighted_value']].rename(
        columns={'weighted_value': col_name}
    )
    return dt

def process_average(dt, col_name, follow_up):
    """
    Calculate the average value of a specified column for each unique 'id' in the DataFrame.

    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to calculate the average for.
    - follow_up: Not used in this function, but can be included for consistency with other
    
    Returns:
    - DataFrame with 'id' and the average value of the specified column.

    """
    dt = dt.groupby('id')[col_name].mean().reset_index(name=col_name)
    return dt

# renew
def process_weighted_average(dt, col_name, follow_up):
    """
    Calculate the weighted average of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to calculate the weighted average for.
    - follow_up: Not used in this function, but can be included for consistency with other
    
    Returns:
    - DataFrame with 'id' and the weighted average value of the specified column.
    
    """
    dt['weighted_value'] = dt[col_name] * dt['weight']
    dt = (
        dt.groupby('id')['weighted_value'].sum()
        / dt.groupby('id')['weight'].sum()
    ).reset_index(name=col_name)
    
    return dt

def process_std(dt, col_name, follow_up):
    """
    Calculate the standard deviation of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to calculate the standard deviation for.
    - follow_up: Not used in this function, but can be included for consistency with other
    
    Returns:
    - DataFrame with 'id' and the standard deviation of the specified column.

    """
    dt = dt.groupby('id')[col_name].std().reset_index(name=col_name)
    return dt

def process_regression(dt, col_name, follow_up):
    """
    Perform a regression-like calculation to find the slope (beta1) of the relationship
    between the 'diff' column and a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to perform regression on.
    - follow_up: Not used in this function, but can be included for consistency with other functions.
    
    Returns:
    - DataFrame with 'id' and the calculated slope (beta1) for the specified    column.
    
    """
    dt = dt.reset_index(drop=True)
    if dt.empty:
        return pd.DataFrame(columns=['id', col_name])
    mean_X = dt['diff'].mean()
    mean_Y = dt[col_name].mean()
    dt['beta1_manual'] = (dt['diff'] - mean_X) * (dt[col_name] - mean_Y)
    dt['id'] = dt['id'].astype(str)
    beta1_numerator = dt.groupby('id')['beta1_manual'].sum()
    beta1_denominator = dt.groupby('id').apply(lambda group: ((group['diff'] - mean_X) ** 2).sum())
    beta1 = beta1_numerator / beta1_denominator
    results = beta1.reset_index(name=col_name)
    
    mean_X= None
    mean_Y= None 
    beta1_numerator= None 
    beta1_denominator= None
    beta1= None
    dt= None
    
    del mean_X,mean_Y,beta1_numerator,beta1_denominator,beta1,dt
    gc.collect()
    
    return results


def calculate_weighted_sum(dt, col_name, follow_up):
    """
    Calculate the weighted sum of a specified column for each unique 'id' in the DataFrame.
    
    Parameters:
    - dt: DataFrame containing the data.
    - col_name: Name of the column to calculate the weighted sum for.
    - follow_up: Not used in this function, but can be included for consistency with other functions.
    
    Returns:
    - DataFrame with 'id' and the weighted sum of the specified column.
    
    """
    dt['weighted_value'] = dt['weight']
    result = dt.groupby('id')['weighted_value'].sum().reset_index(name=col_name)
    
    dt = None
    del dt
    gc.collect()
    
    return result
