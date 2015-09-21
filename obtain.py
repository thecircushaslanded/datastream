"""
This program wraps Datastream from pydatastream.  

Author: Robert Buss robert.a.buss@frb.gob

See LICENSE.txt
"""
import re
import warnings
import datetime as dt
from netrc import netrc

import pandas as pd

from pydatastream import Datastream

class Obtain(Datastream):
    def __init__(self):
        """
        Obtain is used to obtain data from Thomson Reuter's
        Datastream (More specifically, from DataWorks Enterprise).
        
        Obtain interfaces the pydatastream package.
        """
        # Reads in credentials from the user's .netrc file
        rc = netrc()
        uname, account, passwd = rc.authenticators('datastream')
        # Now we are ready to set everything else.
        Datastream.__init__(self, username=uname, password=passwd)

    def from_csv(self, path, date_col=1, **kwargs):
        """
        This loads the Excel file (saved as a csv) that you can download
        from Datastream's Datastream navigator.  The download button is located
        in the upper right corner of the popup window when you are searching 
        for series.  

        Returns:
        --------
        raw: RawData, or list of RawData
        """
        df = pd.read_csv(path, usecols=['Symbol', 'Start Date'],
                parse_dates=[date_col])
        df.columns = ['code', 'start_date']
        start_date = min(df.start_date)
        codes = df.code.tolist()
        if len(codes) <= 16:
            return self.fetch(codes, start_date=start_date, **kwargs)
        else:
            # Split up the codes into chunks of about 16 codes each.
            def codes_split():
                for i in xrange(0, len(codes), 16):
                    yield codes[i:i+16]

            return [self.fetch(code_seg, start_date=start_date, **kwargs) for
                        code_seg in codes_split()]

    def parse_csv(self, path):
        print('We\'ve already got one!')
        pass

    def constituents(self, code, date):
        """Use get_constituents() for now."""
        pass

    def fetch(self, codes, fields=None, freq='D',
            start_date=None, n_years=None, n_days=None):
        """
        Fetches data from Datastream.

        fetch is the workhorse for getting timeseries data.
        It will make request(s) and submit them to TWE using
        the original methods in pydatastream.
        
        *args:
        -----
        codes: str or list
            Datastream will accept many different codes.
            e.g. Datastream Mnemonic, ISIN, SEDOL
            This list should not be longer than 16 codes. 
            Otherwise some codes will be ignored.

        **kwargs:
        ---------
        fields: str or list
            Datastream codes.  These can be very ficky, be certain that 
            all of the codes have all of the fields available.
            If fields are not given, Datastream will return the default
            field (usually price).  The default field will be chosen according
            to the first code.
            The full list of data fields is available at http://dtg.tfn.com/.

        feq: 'D', 'W', 'M', or 'REP'
            Frequency of the data.  Datastream has three frequencies 
            that can be selected: daily ('D'), weekly ('W'), and 
            monthly ('M'). 'REP' is not a true frequency, it 
            the code for a static request.

            If freq is not specified, a default code is selected by 
            Datastream as follows:
                - 'D' for requests less than 5 years
                - 'W' for requests between 5 and 10 years
                - 'M' for requests greater than 10 years

        start_date: datetime object
            When the series should start.

        n_years: int
            Number of years ago the series should start

        n_days: int
            Number of years ago the series should start

        Returns
        -------
        raw: RawData
        """
        query = self._construct_request(codes, 
                fields=fields, 
                start_date=start_date, n_years=n_years, n_days=n_days, 
                freq=freq)

        raw = list(self.request(query))
        raw.append(codes)
        return RawData(raw)


    @staticmethod
    def _construct_request(codes, fields=None, freq=None,
            start_date=None, n_years=None, n_days=None):
        """
        This mimics Datastream.construct_request().
        It gives the user more flexability in generating
        a custom request code.  The details of these codes
        are explained at
        
        http://dtg.tfn.com/data/DataStream.html

        *args:
        -----
        codes: str or list
            Datastream will accept many different codes.
            e.g. Mnemonic, ISIN.

        **kwargs:
        ---------
        fields: str or list
            Datastream codes.  These can be very ficky, be certain that 
            all of the codes have all of the fields available.
            If fields are not given, Datastream will return the default
            field (usually price).
            The full list of data fields is available at http://dtg.tfn.com/.

        feq: 'D', 'W', 'M', or 'REP'
            Frequency of the data.  Datastream has three frequencies 
            that can be selected: daily ('D'), weekly ('W'), and 
            monthly ('M'). 'REP' is not a true frequency, it 
            the code for a static request.

            If freq is not specified, a default code is selected by 
            Datastream as follows:
                - 'D' for requests less than 5 years
                - 'W' for requests between 5 and 10 years
                - 'M' for requests greater than 10 years

        start_date: datetime object
            When the series should start.

        n_years: int
            Number of years ago the series should start

        n_days: int
            Number of years ago the series should start

        NOTE: if no date parameter is specified, Datastream will return
        a one year series.
        """
        request = ''
        # Add the codes to the reqest
        if isinstance(codes, list):
            request += ','.join(codes)
        else:
            request += codes

        # Add the fields to the request
        if fields is not None:
            if isinstance(fields, (str, unicode)):
                request += '~=' + fields
            elif isinstance(fields, list) and len(fields) > 0:
                request += '~=' + ','.join(fields)

        # Add the start_date to the request
        if start_date is not None:
            request += '~' + start_date.strftime('%Y-%m-%d')

        # Add the n_years to the request
        if n_years is not None:
            request += '~-' + str(n_years) + 'Y'

        # Add the n_days to the request
        if n_days is not None:
            request += '~-' + str(n_days) + 'D'

        # Add in frequency to the request.
        if freq is not None:
            request += '~' + freq

        return request


class RawData(object):
    def __init__(self, raw):
        """
        Takes raw data and makes it nice.

        RawData.data is a list of tuples containing the
        non-array-data and array-data for each code.
        If thre was an error, data will be None.
        You can find out more about what went wrong by looking
        at RawData.StatusMessage.

        args:
        -----
        Raw data that comes from Obtain
        """
        # Break raw into it's constituent pieces.
        self.Instrument    = raw[1][1]
        self.StatusType    = raw[2][1]
        self.StatusCode    = raw[3][1]
        self.StatusMessage = raw[4][1]
        self.Fields        = raw[5]
        self.Codes         = raw[6]


        # Check to see if status code is acceptable
        if self.StatusType != 'Connected':
            self.data = None
            # raise Exception('Not connected.') # WARNING
        else:
            Fields = self.Fields[1][0]
            field_dict = dict([self._parseField(field) for field in Fields])

            self.data = []

            # Go through and get data for each code and structure it
            for k in range(len(self.Codes)):
                code = self.Codes[k]
                
                # Get list of fields for the code
                if k == 0:
                    suffix = ''
                    field_names = [x for x in field_dict.keys() if '_' not in x]
                else:
                    suffix = '_'+str(k+1)
                    field_names = [x for x in field_dict.keys() if suffix in x]
                
                # Determine what sort of date should be used
                if 'DATE'+suffix in field_dict.keys():
                    date = field_dict['DATE'+suffix]
                elif 'DATE' in field_dict.keys():
                    date = field_dict['DATE']
                else:
                    date = None

                # Metadata for the code are non-array variables
                non_array = dict()
                # In case of INSTERROR we don't get SYMBOL,
                # but we always have the code, which is the same thing.
                non_array['SYMBOL'] = unicode(code)
                for key in [u'CCY'+suffix, u'DISPNAME'+suffix, 'FREQUENCY'+suffix]:
                    try:
                        non_array[key.replace(suffix, '')] = field_dict[key]
                    except KeyError:
                        pass

                # Filter non-array fields
                non_array_fields = ['CCY', 'DISPNAME', 'FREQUENCY', 'SYMBOL', 
                        'DATE', 'INSTERROR']
                array_fields = [x for x in field_names if not any(
                    [y in x for y in non_array_fields])]
                array_data = {field.replace(suffix, ''):field_dict[field] for 
                        field in array_fields}
                if date is not None:
                    array_data['DATE'] = date

                self.data.append((non_array, array_data))

    def __repr__(self):
        return "Obtain RawData for {}.".format(self.Instrument)

    @staticmethod
    def _parseField(field):
        """ field in a RawData.Field """
        name = unicode(field['Name'])
        try:
            contents = unicode(field['Value'])
        except:
            contents = field['ArrayValue'][0]
        return name, contents

