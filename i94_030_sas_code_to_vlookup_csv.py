"""
This utility aims to disect the semi-structured I94_SAS_Labels_Descriptions.SAS file
And return the lookup logic into tabular Pandas DataFrames and/or CSV files
For these i94 fields: `i94addr`, `i94cit`, `i94res`, `i94mode`, `i94port`, `i94visa`
Note: the lookup for `i94cit` and `i94res` are the same. We group these into `i94cntyl` (country lookup)
"""

import os
import configparser
import pandas as pd
import numpy as np


    
def txt_to_list(txt_path):
    """ Given a path to a text-like file, return a numpy array with all the text lines. """
    with open(txt_path) as f:
        lines = f.readlines()     
    return np.asarray(lines)


def subset_lookup_lines(
    np_array,
    lookup_title: str,
    start_str: str,
    start_pos_offset: int,
    end_str: str,
    end_pos_offset: int):
    """ Given a numpy array of text lines, return a dictionary that includes a the desirable
    subset from the array (based on features) along with meta data. 
    Our objective is to pull out all the code to string mapping portion from the SAS code.
    """
    
    start_pos, end_pos = None, None
    for idx, line in enumerate(np_array):
        #print(line)
        if line == start_str:
            start_pos = idx + start_pos_offset   
            break
            
    for idx, line in enumerate(np_array[start_pos:]):            
        if line == end_str:
            end_pos = start_pos + idx + end_pos_offset
            break
        
    return {
        "lookup_title": lookup_title,
        "start_pos": start_pos,
        "end_pos": end_pos,
        "subset": np_array[start_pos:end_pos].copy()
    }


def array_to_df(np_array, numeric_key=True, sort_by='key'):
    """ Given a numpy array of text lines that contains the semi-structured SAS PROC FORMAT Value statements
    (that contains lookup of categorial codes to texts), return a structured tabular Pandas DataFrame )
    Our objective here is to obtain a tabular code to string mapping portion from the PROC FORMAT VALUE statements
    (from upstream SAS code)
    """
    lookup_list = []
    for idx, line in enumerate(np_array):
        left_right = line.split("=")
        left, right = left_right[0], left_right[1]
        if numeric_key:
            key = int(left.strip())
        else:
            key = left.strip().replace("'", "").strip()
        value = right.strip().replace("'", "").replace(";", "").replace("\n", "").strip()
        lookup_list.append({
            "key": key,
            "value": value
        })
    
    df = pd.DataFrame(lookup_list).sort_values(by=sort_by)
    return df


def build_i94_lookup_dataframes(
        np_array,
        lookup_title: str,
        start_str: str,
        start_pos_offset: int,
        end_str: str,
        end_pos_offset: int,
        numeric_key: bool = False
    ):
    """ Given a numpy array with SAS code proc format value statement lines,
    return a Pandas DataFrame with lookup key and string value """
    
    df_pd = array_to_df(
        subset_lookup_lines(
            np_array,
            lookup_title,
            start_str,
            start_pos_offset,
            end_str,
            end_pos_offset
        )["subset"],
        numeric_key=False,
        sort_by='key'
    )
    return df_pd


def bulk_build_i94_dataframes(input_file) -> dict:
    """ Given I94_SAS_Labels_Descriptions.SAS, return all the lookup dataframes  """
    
    lines = txt_to_list('raw_input_data/i94_proc_format_code_sas/I94_SAS_Labels_Descriptions.SAS')
    
    print(f"Building i94 lookup dataframes from text file: {input_file}")
    df_i94addr = build_i94_lookup_dataframes(lines, 'i94addr', 'value i94addrl\n', 1, '\n', 0)
    df_i94cntyl = build_i94_lookup_dataframes(lines, 'i94cntyl', '  value i94cntyl\n', 1, '\n', 0)
    df_i94port = build_i94_lookup_dataframes(lines, 'i94port', '  value $i94prtl\n', 1, ';\n', 0)
    df_i94mode = build_i94_lookup_dataframes(lines, 'i94mode', 'value i94model\n', 1, '\t\n', 0)
    df_i94visa = build_i94_lookup_dataframes(lines, 'i94visa', '/* I94VISA - Visa codes collapsed into three categories:\n', 1, '*/\n', 0)
    print("Dataframe build Done.")
    
    return {
        "i94addr": df_i94addr,
        "i94cntyl": df_i94cntyl,
        "i94mode": df_i94mode,       
        "i94port": df_i94port,
        "i94visa": df_i94visa,
    }


def bulk_export_i94_csv_files(
        input_file: str,
        output_dir: str,
        output_file_i94addr,
        output_file_i94cntyl,
        output_file_i94port,
        output_file_i94mode,
        output_file_i94visa,
    ):
    """ Given I94_SAS_Labels_Descriptions.SAS, generate all the lookup CSV files """
    
    dataframes_to_export = bulk_build_i94_dataframes(input_file)
    
    print(f"Exporting i94 lookup CSV files to: {output_dir}")
    dataframes_to_export["i94addr"].to_csv(output_file_i94addr, sep=',', index=False)
    dataframes_to_export["i94cntyl"].to_csv(output_file_i94cntyl, sep=',', index=False)
    dataframes_to_export["i94port"].to_csv(output_file_i94port, sep=',', index=False)
    dataframes_to_export["i94mode"].to_csv(output_file_i94mode, sep=',', index=False)
    dataframes_to_export["i94visa"].to_csv(output_file_i94visa, sep=',', index=False)
    print("CSV Export Done.")
    
    return None


def main():
    print("Start Script")
    config_dev = configparser.ConfigParser()
    config_dev.read_file(open('aws_dev.cfg'))
 
    # Directory of the input SAS Code text file
    RAW_I94_SAS_CODE_FILE = config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_SAS_CODE_FILE')
    print(f"RAW_I94_SAS_CODE_FILE: {RAW_I94_SAS_CODE_FILE}")
    
    # Directory of the output lookup CSV files (that this script will auto generate) 
    RAW_I94_LOOKUP_CSV_DIR = config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_DIR')
    RAW_I94_LOOKUP_CSV_FILE_I94ADDR=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94ADDR')
    RAW_I94_LOOKUP_CSV_FILE_I94CNTYL=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94CNTYL')
    RAW_I94_LOOKUP_CSV_FILE_I94PORT=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94PORT')
    RAW_I94_LOOKUP_CSV_FILE_I94MODE=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94MODE')
    RAW_I94_LOOKUP_CSV_FILE_I94VISA=config_dev.get('DATA_PATHS_LOCAL', 'RAW_I94_LOOKUP_CSV_FILE_I94VISA')
    print(f"RAW_I94_LOOKUP_CSV_DIR: {RAW_I94_LOOKUP_CSV_DIR}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94ADDR: {RAW_I94_LOOKUP_CSV_FILE_I94ADDR}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94CNTYL: {RAW_I94_LOOKUP_CSV_FILE_I94CNTYL}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94PORT: {RAW_I94_LOOKUP_CSV_FILE_I94PORT}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94MODE: {RAW_I94_LOOKUP_CSV_FILE_I94MODE}")
    print(f"RAW_I94_LOOKUP_CSV_FILE_I94VISA: {RAW_I94_LOOKUP_CSV_FILE_I94VISA}")
    
    # Auto extract tabular lookup table from the unstructured SAS text file
    bulk_export_i94_csv_files(
        input_file=RAW_I94_SAS_CODE_FILE,
        output_dir=RAW_I94_LOOKUP_CSV_DIR,
        output_file_i94addr=RAW_I94_LOOKUP_CSV_FILE_I94ADDR,
        output_file_i94cntyl=RAW_I94_LOOKUP_CSV_FILE_I94CNTYL,
        output_file_i94port=RAW_I94_LOOKUP_CSV_FILE_I94PORT,
        output_file_i94mode=RAW_I94_LOOKUP_CSV_FILE_I94MODE,
        output_file_i94visa=RAW_I94_LOOKUP_CSV_FILE_I94VISA,
    )

    print("Script Finishes.")
    
    
if __name__ == "__main__":
    main()
