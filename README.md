# datastream

This is an extension of [pydatastream](https://github.com/vfilimonov/pydatastream).  I recommend that you at least skim the readme for pydatastream in order to understand what module does.

## Main differences
 * Addition of `parse_csv` to simplify the process of obtaining large amounts of data.
 * Different version of `fetch` that incorporates more of the options provided by Datastream (See http://dtg.tfn.com/data/DataStream.html for more information).
 * `RawData` objects that are a Python representation of the raw data obtained from Datastream.
 * A function that cleans `RawData` into an easy-to-use (or export) format for data analysis.

