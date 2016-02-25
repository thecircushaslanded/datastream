"""
clean.py
Author: Robert Buss
"""

import pandas as pd

def clean(raw):
    """
    Turns RawData into a DataFrame.

    args:
    -----
    raw: RawData or list of RawData objects
    
    Returns:
    df: Pandas DataFrame
    """
    # Helper function
    def one_code_to_df(code_data):
        """
        Takes an element of RawData.data and
        transforms it into a DataFrame.
        """
        non_array_like, array_like = code_data
        df = pd.DataFrame(array_like)
        for variable in non_array_like:
            df[variable] = non_array_like[variable]
        return df

    if isinstance(raw, list):
        """frames = [pd.concat([one_code_to_df(code_data) for code_data in 
                    raw_piece.data]) for raw_piece in raw]"""
        frames = [pd.concat([one_code_to_df(code_data) for code_data in 
                    raw_piece.data]) for raw_piece in raw 
                    if raw_piece.data != None]
    else:
        frames = [one_code_to_df(code_data) for code_data in raw.data 
                if raw.data != None]
        
    return pd.concat(frames)



