from obtain import Obtain
from pandas import DataFrame, concat
from subprocess import call

from numpy import NaN

from clean import clean

DatastreamDir = "/lcl/if/home/m1rab03/python_modules/datastream/"

def chunks(codes, n):
    """
    Breaks a list of codes into roughly equal n-sized pieces.
    """
    for i in xrange(0, len(codes), n):
        yield codes[i:i+n]

def fetch(codes, n, **kwargs):
    """
    This is a shortcut to downloading Datastream data.  It
    chunks codes into pieces of size n, then fetches and cleans
    them.

    Parameters:
    -----------
    codes: Numpy array or list
    n: int

    Keyword arguments are the same as Obtain.fetch()
        As of December 2015:
        
        **kwargs:
        ---------
        fields: str or list
            Datastream codes.  These can be very ficky, be certain that 
            all of the codes have all of the fields available.
            If fields are not given, Datastream will return the default
            field (usually price).  The default field will be chosen according
            to the first code.
            The full list of data fields is available at http://dtg.tfn.com/.

        static_fields: str or list
            Datastream codes for static variables.  This feature is fragile.

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
    """

    broken = [] # codes that didn't work
    def fetch_chunk(chunk):
        try:
            pieces = [Obtain().fetch(chunk, **kwargs)]
        except TypeError:
            # When the data didn't exist or something else went wrong
            pieces = []
            for piece in chunk:
                try:
                    pieces.append(Obtain().fetch([piece], **kwargs))
                except TypeError:
                    broken.append(piece)
        if len(pieces)>0:
            return pieces

    # List of lists

    try:
        chunked = chunks(codes.dropna(), n)
    except AttributeError:
        chunked = chunks(codes, n)
    chunk_lists = [fetch_chunk(c) for c in chunked]
    flattened = [item for sublist in chunk_lists for item in sublist]
    failed = [item.Codes for item in flattened if item.StatusType==5]
    failed = [item for sublist in failed for item in sublist]
    broken.extend(failed)
    suceeded = [item for item in flattened if item.StatusType!=5]
    try:
        return clean(suceeded), broken
    except:
        return DataFrame(), broken
    # return clean([item for sublist in chunk_lists for item in sublist]), broken

def robust_fetch(codes, out_dir="/lcl/data/scratch/m1rab03/datastream/", 
        fields=["P"], **kwargs):
    """
    This is a shortcut to downloading Datastream data.  It
    downloads codes individually to be more robust.

    Parameters:
    -----------
    codes: numpy array or list
    out_dir: str

    Keyword arguments are the same as Obtain.fetch()
        As of December 2015:
        
        **kwargs:
        ---------
        fields: str or list
            Datastream codes.  These can be very ficky, be certain that 
            all of the codes have all of the fields available.
            If fields are not given, Datastream will return the default
            field (usually price).  The default field will be chosen according
            to the first code.
            The full list of data fields is available at http://dtg.tfn.com/.

        static_fields: str or list
            Datastream codes for static variables.  This feature is fragile.

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
    """
    if out_dir[-1]!='/':
        out_dir+='/'
    call(["rm", out_dir+"file*.tmp"])

    header = True # useful to check things
    k = 0
    data = []
    failed = []
    o = Obtain()
    for code in codes:
        k+=1
        try:
            d = clean(o.fetch([code], **kwargs))
        except:
            print "Total failure: {}".format(code)
            failed.append(code)
        else:
            data.append(d)
            print(1.*k/len(codes))
            if len(set(fields)-set(d.columns))>0:
                print "{} missing {}".format(code, len(set(f)-set(d.columns)))
        if k%10==0 or k==len(codes):
            if len(data)>0:
                df = concat(data)
                # Make sure all columns are present (may be empty)
                for f in fields:
                    try:
                        df[f]
                    except KeyError:
                        df[f] = NaN

                # Standardize column order
                columns = df.columns.tolist()
                columns.sort()
                df = df[columns]
                if k<10:
                    # We could have file0 when there aren't many codes.
                    k = 10
                df.to_csv(out_dir+"file{}.tmp".format(k/10), encoding="utf-8", 
                        index=False, header=header)
            else:
                pass
            data = []


    with open(out_dir+"failed.csv", 'w') as f:
        for code in failed:
            f.write(code+'\n')

    call(["sh", DatastreamDir+"CombineFiles.sh", "-d", out_dir])
