from obtain import Obtain
from clean import clean

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
    codes: list
    n: int

    Keyword arguments are the same as Obtain.fetch()
    """
    return clean([Obtain().fetch(chunk, **kwargs) for chunk in chunks(codes, n)])
