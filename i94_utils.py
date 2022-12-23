import os
import shutil
from datetime import datetime, timedelta



def rmtree_if_exists(dirpath, safe_mode=True): 
    """ Remove directory tree if already exists (so we can start again) """
    
    print(f"dirpath: {dirpath}")
    if os.path.exists(dirpath) and os.path.isdir(dirpath):
        if safe_mode:
            print(f"Directory exists and safe_mode is on. Do nothing.")
        else:
            print(f"Directory exists and safe_mode is off. Remove it recursively.")
            shutil.rmtree(dirpath)
            print("Done.")
    else:
        print(f"Directory does not exist. Do nothing.")
        
        
def convert_datetime(sas_date):
    """ Give a SAS Date, return the equivalent in Python datetime 
    Ref: https://knowledge.udacity.com/questions/66798
    """
    try:
        if (sas_date == 'null'):
            sas_date = 0
        start_cutoff = datetime(1960, 1, 1)
        return start_cutoff + timedelta(days=int(sas_date))
    except:
        return None


if __name__ == "__main__":
    # Mini Unit Tests
    assert convert_datetime(20484) == datetime(2016, 1, 31)
    assert convert_datetime(20484).strftime('%Y-%m-%d') == '2016-01-31'