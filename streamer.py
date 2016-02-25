"""
This program is meant to customize the various options in the 
pydatastream package.  It's primary purpose is to streamline 
working with Datastream to allow for fast development.  As such,
most of the functions are just wrappers that customize the existing
functionality of pydatastream and other packages.
"""
import re
import warnings
import datetime as dt
from netrc import netrc

import numpy as np
import pandas as pd
import bokeh.palettes
# from bokeh.plotting import vplot # This requires a new version of Bokeh.
from bokeh.charts import TimeSeries, output_file, show

from pydatastream import Datastream

class streamer(object):
    def __init__(self):
        """
        Reads in credentials from the user's .netrc file and creates
        a Datastream object to be used in fetching data.
        """
        rc = netrc()
        uname, account, passwd = rc.authenticators('datastream')
        self.DWE = Datastream(username=uname, password=passwd)

    def _fetch_individual_code(self, code, fields=None, **kwargs):
        """
        Internal function to fetch the data.
        It offers two advantages to the original:
            (1) It raises a warning, not an Exception when it has problems.
                This is primarily useful when you have a list of codes and
                want to know when one fails, but move on.
            (2) It renames the colums so that they have the code as a part
                of the column name, not just the field.  This 
        """
        print(code) #TEMP
        try:
            df = self.DWE.fetch(code, fields=fields, **kwargs)
        except Exception, e:
            df = None
            warnings.warn(("Unable to load {} \n".format(code) 
                + str(e.message) + '\n'))
        else:
            if fields==None:
                fields=['D']
            df.columns = [code + "({})".format(column) for column in df.columns]
        return df

    def _decode_columns(self, human_codes, inverse=False):
        """
        Changes column names created by fetch to be
        human readable strings. For this to run,
        self.content cannot be empty.
        
        Parameters:
        -----------
        codes: dictionary
           Each entry is of the form "code":"Human readable code"
        inverse: bool (optional)
           Exchanges the keys and codes in the dictionary.
        """
        def strip_fields(code):
            """ 
            Takes a code of the format XXXX(Y) and returns 
            the part before the (Y).
            """
            return re.findall('.*\(', code)[0][:-1]
        def get_fields(code):
            """ 
            Takes a code of the format XXXX(Y) and returns 
            the part after XXX.
            """
            return re.findall('([A-Z]*)', code)[0]
        def inverse_dict(d):
            """
            Function to reverse the keys and information
            in a dictionary.  
            """
            return {v:k for k,v in d.items()}

        if inverse:
            human_codes = inverse_dict(human_codes)

        self.content.columns = [human_codes[strip_fields(column)]+' '+
                get_fields(column) for column in self.content.columns] 

    def fetch(self, codes, fields=None, date_from=None, start_date=None, 
            human_columns=None, **kwargs):
        """
        Wrapper for the Datastream fetch to generate a 
        DataFrame with the codes as columns. This uses
        fetch_individual_code and then concatenates the
        data.  For more information, see the documentation
        for the pydatastream fetch command.

        Parameters:
        -----------
        codes: list
            A list of symbols in Datastream.

        fields: list
        start_date: string or dictionary (optional)
            Single date (or dictionary) to start the series at.
            If it is a dictionary, it should be in the form 'code':'start_date'
            Missing entries will be regarded as None.

        human_columns: dict (optional)
            Dictionary of column names.  This must be in the form
            'code':'Human Readable Code Name', and cannot be missing
            any of the codes.

        Returns:
        --------
        data: DataFrame

        """
        # Sometimes this is used to fetch a single ticker.
        codes=list(codes)
        # I like to use start_date, not date_from.  Both should work.
        if date_from == None and start_date != None:
            date_from = start_date
        
        if start_date == None:
            raw_data = [self._fetch_individual_code(code, 
                fields=fields, 
                **kwargs) 
                for code in codes]
        elif type(date_from) == str:
            raw_data = [self._fetch_individual_code(code, 
                fields=fields, date_from=date_from, **kwargs) for code in codes]
        elif type(date_from) == dict:
            # When the code is not in the dictionary, date_from.get(code) 
            # returns None instead of a KeyError.
            raw_data = [self._fetch_individual_code(code, fields=fields, 
                date_from=date_from.get(code), **kwargs) for code in codes]

        # Remove entries that are None
        raw_data = [df for df in raw_data if type(df)==pd.core.frame.DataFrame]

        # Concatenate the results
        data = pd.concat(raw_data, axis=1)
        
        self.content = data
        if type(human_columns) == dict:
            data = self._decode_columns(human_columns)


        return data

    def parse_csv(self, path, human_cols=True, date_col=1, **kwargs):
        """
        This parses the Excel file (saved as a csv) that you can download
        from Datastream's Datastream navigator.  The download button is located
        in the upper right corner of the popup window when you are searching 
        for series.  
        """
        df = pd.read_csv(path, usecols=['Symbol', 'Start Date', 'Full Name'],
                parse_dates=[date_col])
        df.columns = ['code', 'date_from', 'name']
        start_date = dict(zip(df.code, df.date_from))
        if human_cols:
            human_columns = dict(zip(df.code, df.name))
            self.fetch(df.code, start_date=start_date, 
                    human_columns=human_columns, **kwargs)
        else:
            self.fetch(df.code, start_date=start_date, **kwargs)
        return self.content

    def parse_csv_buffered(self, path, human_cols=False, buffer=10, **kwargs):
        df = pd.read_csv(path, usecols=['Symbol', 'Start Date', 'Full Name'],
                parse_dates=[1])
        df.columns = ['code', 'date_from', 'name']
        start_date = dict(zip(df.code, df.date_from))
        human_columns = dict(zip(df.code, df.name))

        total_len = len(df)
        n = 0
        while n < total_len-buffer:
            if human_cols:
                yield self.fetch(df.code.iloc[n:n+buffer], 
                        start_date=start_date, human_columns=human_columns, 
                        **kwargs)
            else:
                yield self.fetch(df.code.iloc[n:n+buffer], 
                        start_date=start_date, 
                        **kwargs)
            n += buffer

        while total_len-buffer<= n:
            if human_cols:
                yield self.fetch(df.code.iloc[n:], 
                        start_date=start_date, human_columns=human_columns, 
                        **kwargs)
            else:
                yield self.fetch(df.code.iloc[n:], 
                        start_date=start_date, 
                        **kwargs)

    def plot(self, pct=False,
            output_file_path='temp_plot.html', title="", legend=True):
        """
        Allows the user to plot the timeseries data in self.content 
        using Bokeh. 

        Parameters
        ----------
        pct: bool
            Transformes the data to be percent change.
        output_file_path: str
            Path, including the name, for the output file.
        title: str
            The title of the graph and the html page.
        legend: bool
             Whether to include the legend or not.
        """
        # Output to static HTML file
        output_file(output_file_path, title=title)

        no_cols_needed = len(self.content.columns)
        if no_cols_needed == 3: Spectral = bokeh.palettes.Spectral3
        if no_cols_needed == 4: Spectral = bokeh.palettes.Spectral4
        if no_cols_needed == 5: Spectral = bokeh.palettes.Spectral5
        if no_cols_needed == 6: Spectral = bokeh.palettes.Spectral6
        if no_cols_needed == 7: Spectral = bokeh.palettes.Spectral7
        if no_cols_needed == 8: Spectral = bokeh.palettes.Spectral8
        if no_cols_needed == 9: Spectral = bokeh.palettes.Spectral9
        if no_cols_needed == 10: Spectral = bokeh.palettes.Spectral10
        if no_cols_needed >= 11: Spectral = bokeh.palettes.Spectral11


        data = self.content
        # Bokeh stumbles if the series starts with a nan.  
        # Hopefully will be fixed in Bokeh 0.9.4
        data = data.dropna(thresh=len(data.columns), axis=1)
        data.iloc[0] = data.iloc[0].fillna(0)

        p = TimeSeries(data, legend=legend, title=title, 
                width=800, height=350)
                # width=800, height=350, palette=Spectral)

        if pct:
            data2 = self.content.pct_change()
            data2 = data2.dropna(thresh=len(data2.columns), axis=1)
            data2.iloc[0] = data2.iloc[0].fillna(0)
            p2 = TimeSeries(data2, legend=legend, title="Percent Change", 
                width=800, height=350)
                # width=800, height=350, palette=Spectral)
            # show(vplot(p,p2))
            show(p2)
        else:
            show(p)

    def request(self, request_string):
        """
        This is used to make custom requests.  See
        the pydatastream documentation and Datastream
        documentation to create a request_string.
        """
        raw = self.DWE.request(request_string)
        return self.DWE.extract_data(raw)

    def get_constituents(self, index_ticker, date=None):
        """
        Wraps the pydatastream function and trims out 
        columns that I do not find useful.
        """
        list = self.DWE.get_constituents(index_ticker, date=date)
        return list

    def to_csv(self, filename, **kwargs):
        """
        Wrapper for the Pandas DataFrame to_csv() method.
        """
        self.content.to_csv(filename, **kwargs)




if __name__=='__main__':
    """stream = streamer()
    from global_idx import get_world
    World = get_world()
    stream.fetch(World.values(), fields=["PI"])
    stream._decode_columns(World, inverse=True)
    stream.plot(pct=True)"""
    """stream2 = streamer()
    stream2.parse_csv('Thailand5.csv', human_cols=False)
    stream2.plot(pct=True)"""
    stream = streamer()
    n = 300
    for f in stream.parse_csv_buffered('ChinaFirmsDatastreamCodes_end.csv', 
            buffer=150):
        stream.content.to_csv('ChinaFirmNew{}.csv'.format(n))
        print(n)
        n += 1


