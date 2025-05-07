--- /dev/null
+++ b/your_workspace/automated_data_cleaner.py
@@ -0,0 +1,404 @@
+import pandas as pd
+import numpy as np
+from typing import Union
+def load_data(file_path: str) -> Union[pd.DataFrame, None]:
+def load_data(file_path: str) -> pd.DataFrame | None:
+    """
+    Loads data from a specified file path into a pandas DataFrame.
+    Currently supports CSV. Can be extended for other formats (Excel, SQL, etc.).
+
+    Args:
+        file_path (str): The path to the data file.
+
+    Returns:
+        pd.DataFrame | None: The loaded DataFrame, or None if loading fails.
+    """
+    try:
+        # Assuming CSV for now, can be made more flexible
+        if file_path.endswith('.csv'):
+            df = pd.read_csv(file_path)
+        # elif file_path.endswith('.xlsx'):
+        #     df = pd.read_excel(file_path)
+        else:
+            print(f"Error: Unsupported file type for {file_path}. Please use .csv or extend this function.")
+            return None
+        print(f"Data loaded successfully from '{file_path}'")
+        return df.copy()  # Work on a copy
+    except FileNotFoundError:
+        print(f"Error: The file '{file_path}' was not found.")
+        return None
+    except Exception as e:
+        print(f"An error occurred during data loading from '{file_path}': {e}")
+        return None
+def inspect_data(df: Union[pd.DataFrame, None], df_name: str = "DataFrame"):
+def inspect_data(df: pd.DataFrame | None, df_name: str = "DataFrame"):
+    """
+    Prints basic inspection information about the DataFrame.
+
+    Args:
+        df (pd.DataFrame | None): The DataFrame to inspect.
+        df_name (str): A name for the DataFrame for logging purposes.
+    """
+    if df is None:
+        print(f"{df_name} is None. Skipping inspection.")
+        return
+
+    print(f"\n--- Inspection of {df_name} ---")
+    print("First 5 rows:\n", df.head())
+    print("\nDataFrame Info:")
+    df.info()
+    print("\nDescriptive Statistics (including non-numeric):\n", df.describe(include='all'))
+    print("\nShape of DataFrame:", df.shape)
+    print("\nMissing values per column:\n", df.isnull().sum())
+    print(f"--- End of {df_name} Inspection ---")
+def handle_missing_values(df: Union[pd.DataFrame, None],
+                          numerical_cols_median: Union[list, None] = None,
+                          categorical_cols_mode: Union[list, None] = None,
+                          drop_cols_threshold: Union[float, None] = None,
+                          drop_rows_subset: Union[list, None] = None) -> Union[pd.DataFrame, None]:
+                          drop_rows_subset: list | None = None) -> pd.DataFrame | None:
+    """
+    Handles missing values in the DataFrame by imputation or dropping.
+
+    Args:
+        df (pd.DataFrame | None): The input DataFrame.
+        numerical_cols_median (list, optional): Numerical columns to impute with median.
+        categorical_cols_mode (list, optional): Categorical columns to impute with mode.
+        drop_cols_threshold (float, optional): Threshold (0.0-1.0) to drop columns.
+                                               e.g., 0.7 means drop columns with >70% missing.
+        drop_rows_subset (list, optional): List of columns to consider for dropping rows with NaNs.
+                                           If None, this specific row dropping is skipped.
+                                           If an empty list, it might not behave as expected with dropna's subset.
+                                           Best to provide specific columns or handle "all columns" explicitly if needed.
+
+    Returns:
+        pd.DataFrame | None: The DataFrame with missing values handled.
+    """
+    if df is None: return None
+    print("\n--- Handling Missing Values ---")
+    df_processed = df.copy()
+
+    if numerical_cols_median:
+        for col in numerical_cols_median:
+            if col in df_processed.columns:
+                if pd.api.types.is_numeric_dtype(df_processed[col]):
+                    median_val = df_processed[col].median()
+                    df_processed[col].fillna(median_val, inplace=True)
+                    print(f"Imputed missing values in numerical column '{col}' with median ({median_val}).")
+                else:
+                    print(f"Warning: Column '{col}' is not numeric. Skipping median imputation.")
+            else:
+                print(f"Warning: Numerical column '{col}' for median imputation not found.")
+
+    if categorical_cols_mode:
+        for col in categorical_cols_mode:
+            if col in df_processed.columns:
+                # Ensure it's treated as object/category for mode calculation
+                if not (pd.api.types.is_categorical_dtype(df_processed[col]) or pd.api.types.is_object_dtype(df_processed[col])):
+                     print(f"Warning: Column '{col}' is not explicitly categorical or object type. Attempting mode imputation.")
+                mode_val_series = df_processed[col].mode()
+                if not mode_val_series.empty:
+                    df_processed[col].fillna(mode_val_series[0], inplace=True)
+                    print(f"Imputed missing values in column '{col}' with mode ('{mode_val_series[0]}').")
+                else:
+                    df_processed[col].fillna('Unknown', inplace=True) # Fallback
+                    print(f"Mode for column '{col}' was empty or all NaN. Imputed with 'Unknown'.")
+            else:
+                print(f"Warning: Categorical column '{col}' for mode imputation not found.")
+
+    if drop_cols_threshold is not None:
+        initial_cols_count = df_processed.shape[1]
+        # thresh requires at least this many non-NA values
+        min_non_na = int((1.0 - drop_cols_threshold) * len(df_processed))
+        df_processed.dropna(axis=1, thresh=min_non_na, inplace=True)
+        cols_dropped = initial_cols_count - df_processed.shape[1]
+        if cols_dropped > 0:
+            print(f"Dropped {cols_dropped} columns with more than {drop_cols_threshold*100:.1f}% missing values.")
+
+    if drop_rows_subset: # Only proceed if drop_rows_subset is a non-empty list
+        initial_rows_count = df_processed.shape[0]
+        df_processed.dropna(axis=0, how='any', subset=drop_rows_subset, inplace=True)
+        rows_dropped = initial_rows_count - df_processed.shape[0]
+        if rows_dropped > 0:
+            print(f"Dropped {rows_dropped} rows with missing values in subset: {drop_rows_subset}.")
+
+    print(f"Missing values after handling:\n{df_processed.isnull().sum().loc[lambda x: x > 0]}") # Show only cols with remaining NaNs
+    print("--- End of Missing Value Handling ---")
+    return df_processed
+def handle_outliers_iqr(df: Union[pd.DataFrame, None],
+def handle_outliers_iqr(df: pd.DataFrame | None,
+                        numerical_cols: list,
+                        cap_outliers: bool = True,
+                        remove_outliers: bool = False,
+                        factor: float = 1.5) -> pd.DataFrame | None:
+    """
+    Handles outliers in specified numerical columns using the IQR method.
+    If both cap and remove are True, remove takes precedence.
+
+    Args:
+        df (pd.DataFrame | None): The input DataFrame.
+        numerical_cols (list): Numerical column names to check for outliers.
+        cap_outliers (bool): If True and remove_outliers is False, cap outliers to IQR bounds.
+        remove_outliers (bool): If True, remove rows with outliers.
+        factor (float): IQR factor to determine outlier bounds (default 1.5).
+
+    Returns:
+        pd.DataFrame | None: The DataFrame with outliers handled.
+    """
+    if df is None: return None
+    print("\n--- Handling Outliers (IQR Method) ---")
+    df_processed = df.copy()
+
+    for col in numerical_cols:
+        if col in df_processed.columns and pd.api.types.is_numeric_dtype(df_processed[col]):
+            Q1 = df_processed[col].quantile(0.25)
+            Q3 = df_processed[col].quantile(0.75)
+            IQR = Q3 - Q1
+            lower_bound = Q1 - factor * IQR
+            upper_bound = Q3 + factor * IQR
+
+            outliers_mask = (df_processed[col] < lower_bound) | (df_processed[col] > upper_bound)
+            outliers_count = outliers_mask.sum()
+            print(f"Column '{col}': Q1={Q1:.2f}, Q3={Q3:.2f}, IQR={IQR:.2f}, Lower={lower_bound:.2f}, Upper={upper_bound:.2f}. Found {outliers_count} potential outliers.")
+
+            if outliers_count > 0:
+                if remove_outliers:
+                    initial_rows = len(df_processed)
+                    df_processed = df_processed[~outliers_mask]
+                    print(f"Removed {initial_rows - len(df_processed)} rows with outliers in '{col}'.")
+                elif cap_outliers:
+                    df_processed[col] = np.where(df_processed[col] < lower_bound, lower_bound, df_processed[col])
+                    df_processed[col] = np.where(df_processed[col] > upper_bound, upper_bound, df_processed[col])
+                    print(f"Capped outliers in '{col}' to bounds [{lower_bound:.2f}, {upper_bound:.2f}].")
+        elif col not in df_processed.columns:
+            print(f"Warning: Numerical column '{col}' for outlier handling not found.")
+        else:
+            print(f"Warning: Column '{col}' is not numeric. Skipping outlier handling.")
+    print("--- End of Outlier Handling ---")
+    return df_processed
+def correct_data_types(df: Union[pd.DataFrame, None],
+                       to_numeric_cols: Union[list, None] = None,
+                       to_datetime_cols: Union[list, None] = None,
+                       to_category_cols: Union[list, None] = None,
+                       numeric_clean_chars: Union[str, None] = None) -> Union[pd.DataFrame, None]:
+                       numeric_clean_chars: str | None = None) -> pd.DataFrame | None:
+    """
+    Corrects data types for specified columns.
+
+    Args:
+        df (pd.DataFrame | None): The input DataFrame.
+        to_numeric_cols (list, optional): Columns to convert to numeric.
+        to_datetime_cols (list, optional): Columns to convert to datetime.
+        to_category_cols (list, optional): Columns to convert to category.
+        numeric_clean_chars (str, optional): Regex pattern of chars to remove before numeric conversion (e.g., r'[$,]').
+
+    Returns:
+        pd.DataFrame | None: The DataFrame with corrected data types.
+    """
+    if df is None: return None
+    print("\n--- Correcting Data Types ---")
+    df_processed = df.copy()
+
+    if to_numeric_cols:
+        for col in to_numeric_cols:
+            if col in df_processed.columns:
+                if numeric_clean_chars and df_processed[col].dtype == 'object':
+                    # Ensure it's string type before using .str accessor
+                    df_processed[col] = df_processed[col].astype(str).str.replace(numeric_clean_chars, '', regex=True)
+                    print(f"Cleaned characters '{numeric_clean_chars}' from column '{col}'.")
+                original_nan_count = df_processed[col].isnull().sum()
+                df_processed[col] = pd.to_numeric(df_processed[col], errors='coerce')
+                new_nans = df_processed[col].isnull().sum() - original_nan_count
+                print(f"Converted column '{col}' to numeric. Additional NaNs introduced by coercion: {new_nans}")
+            else:
+                print(f"Warning: Column '{col}' for numeric conversion not found.")
+
+    if to_datetime_cols:
+        for col in to_datetime_cols:
+            if col in df_processed.columns:
+                try:
+                    original_nat_count = df_processed[col].isnull().sum()
+                    df_processed[col] = pd.to_datetime(df_processed[col], errors='coerce')
+                    new_nats = df_processed[col].isnull().sum() - original_nat_count
+                    print(f"Converted column '{col}' to datetime. Additional NaTs introduced by coercion: {new_nats}")
+                except Exception as e:
+                    print(f"Error converting column '{col}' to datetime: {e}. It may remain unchanged or coerced to NaT.")
+            else:
+                print(f"Warning: Column '{col}' for datetime conversion not found.")
+
+    if to_category_cols:
+        for col in to_category_cols:
+            if col in df_processed.columns:
+                df_processed[col] = df_processed[col].astype('category')
+                print(f"Converted column '{col}' to category.")
+            else:
+                print(f"Warning: Column '{col}' for category conversion not found.")
+
+    print("Data types after corrections:\n", df_processed.dtypes)
+    print("--- End of Data Type Correction ---")
+    return df_processed
+def standardize_text_values(df: Union[pd.DataFrame, None],
+                            text_cols_to_clean: Union[list, None] = None,
+                            value_mappings: Union[dict, None] = None) -> Union[pd.DataFrame, None]:
+                            value_mappings: dict | None = None) -> pd.DataFrame | None:
+    """
+    Standardizes text values (lowercase, strip) and applies custom value mappings.
+
+    Args:
+        df (pd.DataFrame | None): The input DataFrame.
+        text_cols_to_clean (list, optional): Text columns to lowercase and strip whitespace.
+        value_mappings (dict, optional): Dict where keys are column names and values are
+                                         dicts of old_value: new_value.
+                                         Example: {'country': {'U.S.': 'USA', 'Canda': 'Canada'}}
+
+    Returns:
+        pd.DataFrame | None: The DataFrame with standardized text values.
+    """
+    if df is None: return None
+    print("\n--- Standardizing Text and Values ---")
+    df_processed = df.copy()
+
+    if text_cols_to_clean:
+        for col in text_cols_to_clean:
+            if col in df_processed.columns:
+                if df_processed[col].dtype == 'object':
+                    df_processed[col] = df_processed[col].str.lower().str.strip()
+                    print(f"Standardized text column '{col}' (lowercase, strip).")
+                else:
+                    print(f"Warning: Column '{col}' is not object type. Skipping text cleaning for it.")
+            else:
+                print(f"Warning: Text column '{col}' for cleaning not found.")
+
+    if value_mappings:
+        for col, mapping_dict in value_mappings.items():
+            if col in df_processed.columns:
+                df_processed[col] = df_processed[col].replace(mapping_dict)
+                print(f"Applied value mappings to column '{col}'. Mapped values: {len(mapping_dict)}")
+            else:
+                print(f"Warning: Column '{col}' for value mapping not found.")
+
+    print("--- End of Text and Value Standardization ---")
+    return df_processed
+def remove_duplicates(df: Union[pd.DataFrame, None],
+                      subset_cols: Union[list, None] = None,
+                      keep: str = 'first') -> Union[pd.DataFrame, None]:
+                      keep: str = 'first') -> pd.DataFrame | None:
+    """
+    Removes duplicate rows from the DataFrame.
+
+    Args:
+        df (pd.DataFrame | None): The input DataFrame.
+        subset_cols (list, optional): Columns to consider for identifying duplicates. All columns if None.
+        keep (str): Which duplicate to keep ('first', 'last', False for all).
+
+    Returns:
+        pd.DataFrame | None: The DataFrame with duplicates removed.
+    """
+    if df is None: return None
+    print("\n--- Removing Duplicates ---")
+    df_processed = df.copy()
+    initial_rows = len(df_processed)
+    df_processed.drop_duplicates(subset=subset_cols, keep=keep, inplace=True)
+    rows_dropped = initial_rows - len(df_processed)
+    if rows_dropped > 0:
+        print(f"Removed {rows_dropped} duplicate rows (subset: {subset_cols if subset_cols else 'all'}, keep: '{keep}').")
+    else:
+        print("No duplicate rows found based on criteria.")
+    print(f"DataFrame shape after dropping duplicates: {df_processed.shape}")
+    print("--- End of Duplicate Removal ---")
+    return df_processed
+
+def save_data(df: pd.DataFrame | None, output_file_path: str):
+    """
+    Saves the DataFrame to a CSV file.
+
+    Args:
+        df (pd.DataFrame | None): The DataFrame to save.
+        output_file_path (str): The path to save the cleaned data.
+    """
+    if df is None:
+        print("DataFrame is None. Skipping save operation.")
+        return
+    try:
+        df.to_csv(output_file_path, index=False)
+        print(f"\nCleaned data saved successfully to '{output_file_path}'")
+    except Exception as e:
+        print(f"An error occurred while saving the cleaned data to '{output_file_path}': {e}")
+
+def main_cleaning_workflow(config: dict):
+    """
+    Main function to orchestrate the data cleaning workflow based on a configuration.
+
+    Args:
+        config (dict): A dictionary containing configuration for the cleaning process.
+                       See example `cleaning_config` in the `if __name__ == "__main__":` block.
+    """
+    print("Starting Data Cleaning Workflow...")
+
+    df = load_data(config['input_file_path'])
+    if df is None:
+        print("Exiting due to data loading failure.")
+        return
+
+    inspect_data(df, "Original DataFrame")
+
+    # Sequentially apply cleaning steps based on config
+    if 'missing_values_config' in config and config['missing_values_config'].get('enabled', True):
+        mv_conf = config['missing_values_config']
+        df = handle_missing_values(df, **{k:v for k,v in mv_conf.items() if k != 'enabled'})
+
+    if 'outlier_config' in config and config['outlier_config'].get('enabled', True) and df is not None:
+        out_conf = config['outlier_config']
+        df = handle_outliers_iqr(df, **{k:v for k,v in out_conf.items() if k != 'enabled'})
+
+    if 'type_correction_config' in config and config['type_correction_config'].get('enabled', True) and df is not None:
+        tc_conf = config['type_correction_config']
+        df = correct_data_types(df, **{k:v for k,v in tc_conf.items() if k != 'enabled'})
+
+    if 'standardization_config' in config and config['standardization_config'].get('enabled', True) and df is not None:
+        std_conf = config['standardization_config']
+        df = standardize_text_values(df, **{k:v for k,v in std_conf.items() if k != 'enabled'})
+
+    if 'duplicate_removal_config' in config and config['duplicate_removal_config'].get('enabled', True) and df is not None:
+        dr_conf = config['duplicate_removal_config']
+        df = remove_duplicates(df, **{k:v for k,v in dr_conf.items() if k != 'enabled'})
+
+    if df is not None:
+        inspect_data(df, "Cleaned DataFrame")
+        save_data(df, config['output_file_path'])
+    else:
+        print("Cleaning process resulted in a None DataFrame. Output not saved.")
+
+    print("\nData Cleaning Workflow Completed.")
+
+if __name__ == "__main__":
+    # --- Configuration for the Cleaning Process ---
+    # Adjust this dictionary to define your cleaning steps and parameters.
+    # Column names used here are examples; replace them with your actual column names.
+    cleaning_config = {
+        'input_file_path': 'your_dataset.csv',  # Replace with your input file path
+        'output_file_path': 'cleaned_dataset.csv', # Replace with your desired output file path
+
+        'missing_values_config': {
+            'enabled': True,
+            'numerical_cols_median': ['age', 'salary'], # Columns to impute with median
+            'categorical_cols_mode': ['department', 'city'], # Columns to impute with mode
+            'drop_cols_threshold': 0.8, # Drop columns with >80% missing values
+            'drop_rows_subset': ['user_id', 'email'] # Drop rows if 'user_id' OR 'email' is NaN
+        },
+
+        'outlier_config': {
+            'enabled': True,
+            'numerical_cols': ['price', 'quantity', 'age'], # Columns to check for outliers
+            'cap_outliers': True,       # Cap outliers to IQR bounds
+            'remove_outliers': False,   # If True, removes rows with outliers (overrides cap)
+            'factor': 1.5               # IQR factor
+        },
+
+        'type_correction_config': {
+            'enabled': True,
+            'to_numeric_cols': ['amount_str', 'item_price'],
+            'numeric_clean_chars': r'[$,€]', # Regex for chars to remove before numeric conversion
+            'to_datetime_cols': ['order_date', 'signup_date'],
+            'to_category_cols': ['status_code', 'product_category']
+        },
+
+        'standardization_config': {
+            'enabled': True,
+            'text_cols_to_clean': ['product_name', 'customer_feedback'], # Lowercase & strip
+            'value_mappings': {
+                'country': { # Column name
+                    'U.S.': 'USA',
+                    'United States': 'USA',
+                    'America': 'USA',
+                    'CAN': 'Canada',
+                    'Canda': 'Canada' # Typo correction
+                },
+                'gender': {
+                    'M': 'Male',
+                    'F': 'Female',
+                    '0': 'Unknown',
+                    '1': 'Unknown'
+                }
+            }
+        },
+
+        'duplicate_removal_config': {
+            'enabled': True,
+            'subset_cols': ['transaction_id', 'customer_id'], # Consider these cols for duplicates
+            'keep': 'first' # Keep 'first', 'last', or False (drop all duplicates)
+        }
+    }
+
+    # Run the cleaning workflow
+    main_cleaning_workflow(cleaning_config)
+
+    # --- How to Use ---
+    # 1. Save this script as a Python file (e.g., automated_data_cleaner.py).
+    # 2. Modify the `cleaning_config` dictionary at the bottom of the script:
+    #    - Set `input_file_path` to your raw data file.
+    #    - Set `output_file_path` to where you want the cleaned data saved.
+    #    - Adjust the column names and parameters within each step's configuration
+    #      (e.g., `numerical_cols_median`, `text_cols_to_clean`, `value_mappings`).
+    #    - Set `'enabled': False` for any step you want to skip.
+    # 3. Place your data file (e.g., 'your_dataset.csv') in the same directory as the script,
+    #    or provide the full path.
+    # 4. Run the script from your terminal: python automated_data_cleaner.py
+    #
+    # --- Further Enhancements ---
+    # - Load configuration from an external file (JSON, YAML) for better separation.
+    # - Add more sophisticated imputation methods (e.g., KNNImputer, IterativeImputer from scikit-learn).
+    # - Implement more advanced outlier detection techniques.
+    # - Integrate logging to a file instead of just printing to console.
+    # - Add unit tests for each cleaning function.
+    # - Extend `load_data` and `save_data` to support more file formats (Excel, Parquet, SQL databases).
