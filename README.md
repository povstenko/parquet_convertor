# Data Files Convertor

This script allows the user to convert different data types. 

This tool accepts comma separated value files (.csv) as well as apache parquet (.parquet) files. It is assumed that the first row of the spreadsheet is the location of the columns.

This script requires that `pandas`, `argparse`, `pyarrow`, `csv` and `json` be installed within the Python environment you are running this script in.

This file can also be imported as a module and contains the following functions:

* csv_to_parquet - convert csv to parquet and save to file
* parquet_to_csv - convert parquet to csv and save to file
* csv_to_json - convert csv to json and save to file
* parquet_schema - returns schema of parquet file
* add_filename_suffix - returns filename string with added suffix for filename and change extension
* is_file_ext_correct - returns returns True if filename has correct file extension and prints message otherwise
* main - the main function of the script