"""Data Files Convertor

This script allows the user to convert different data types. 

This tool accepts comma separated value files (.csv) as well as apache parquet
(.parquet) files. It is assumed that the first row of the spreadsheet is the
location of the columns.

This script requires that `pandas`, `pyarrow`, `argparse` and `time` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * convert_csv_to_parquet - convert csv to parquet and save to file
    * convert_parquet_to_csv - convert parquet to csv and save to file
    * get_get_parquet_schema - returns schema of parquet file
    * add_filename_suffix - returns filename string with added suffix for filename and change extension
    * is_file_ext_correct - returns returns True if filename has correct file extension and prints message otherwise
    * print_success_message - prints message of successfull convertion with elapsed time
    * main - the main function of the script
"""

# import the necessary packages
import pandas as pd
import pyarrow
import argparse
import time

# define functions for convert
def convert_csv_to_parquet(csv_path: str, parquet_path: str, delimiter=','):
    """Convert csv to parquet and save to file
    
    Parameters
    ----------
    csv_path: str
        The file name of the csv
    parquet_path: str
        The file name of the parquet
    delimiter: str, optional
        Delimiter to use in parsing engine (default is ',')
    """
    try:
        df = pd.read_csv(csv_path, sep=delimiter)
        df.to_parquet(parquet_path)
    except Exception as e:
        print(e)


def convert_parquet_to_csv(parquet_path: str, csv_path: str, delimiter=','):
    """Convert parquet to csv and save to file
    
    Parameters
    ----------
    parquet_path: str
        The file name of the parquet
    csv_path: str
        The file name of the csv
    delimiter: str, optional
        Delimiter to use in parsing engine (default is ',')
    """
    try:
        df = pd.read_parquet(parquet_path)
        df.to_csv(csv_path, sep=delimiter, index=False)
    except Exception as e:
        print(e)


def get_parquet_schema(parquet_path: str) -> str:
    """Get schema of parquet file
    
    Parameters
    ----------
    parquet_path: str
        The file name of the parquet
    
    Returns
    -------
    str
        a string of parquet schema
    """
    try:
        df = pd.read_parquet(parquet_path)
        schema = pyarrow.Table.from_pandas(df=df).schema
    except Exception as e:
        print(e)
    else:
        return schema


# define functions for working with filenames
def add_filename_suffix(filename: str, suffix: str, extension: str) -> str:
    """Add suffix for filename and change extension
    
    Parameters
    ----------
    filename: str
        The file name string
    suffix: str
        The suffix which should be added at the end of filename
    extension: str
        New extension of filename
    
    Returns
    -------
    str
        filename string with added suffix and new file extension
    """
    try:
        return filename.split('.')[0] + '_' + suffix + '.' + extension
    except Exception as e:
        print(e)


def is_file_ext_correct(parameter: str, filename: str, extension: str) -> bool:
    """Returns True if filename has correct file extension and prints message otherwise"
    
    Parameters
    ----------
    parameter: str
        The name of parameter used to print in error message
    filename: str
        The file name string
    extension: str
        File extension used to compare with filename
    
    Returns
    -------
    bool
        A flag used to determinate is the given filename has correct extension
    """
    try:
        assert filename.split('.')[1] == extension
    except:
        print(f'Wrong argument for --{parameter}. You must specify *.{extension} file for input')
        return False
    else:
        return True
    


# define other functions
def print_success_message(inputFilename: str, outputFilename: str, time_start:float):
    """Print final message of successfully converted files with elapsed time

    Parameters
    ----------
    inputFilename : str
        Name of input file
    outputFilename : str
        Name of converted output file
    time_start : float
        Start of time countdown used for calculating elapsed time
    """
    time_elapsed = round((time.perf_counter() - time_start), 4)
    print(f'Successfully converted from {inputFilename} to {outputFilename} in {time_elapsed} secs')

def construct_argument_parser() -> dict:
    """Construct the argument parser and get the arguments

    Returns
    -------
    dict
        Dictionary of arguments and paramenters
    """
    ap = argparse.ArgumentParser(
        description=__doc__)
    ap.add_argument("-cp", "--csv2parquet", type=str,
                    help="Convert csv to parquet. Set input csv filename string (example: data.csv)")
    ap.add_argument("-pc", "--parquet2csv", type=str,
                    help="Convert parquet to csv. Set input parquet filename string (example: data.parquet)")
    ap.add_argument("-s", "--get_schema", type=str,
                    help="Get schema of parquet file. Set input parquet filename string (example: data.parquet)")
    ap.add_argument("-o", "--output", type=str,
                    help="Set output file name without extension (example: newfile)")
    ap.add_argument("-d", "--delimiter", type=str, default=",",
                    help="Set delimiter for csv file (default: ,)")
    
    return vars(ap.parse_args())

def main():
    # save start time for calculating
    time_start = time.perf_counter()
    
    args = construct_argument_parser()
    
    # check convert option
    if args['csv2parquet']:
        # convert csv to parquet
        if is_file_ext_correct('csv2parquet', args['csv2parquet'], 'csv'):
            inputFilename = args['csv2parquet']
            
            # check output filename argument
            if args['output']:
                outputFilename = args['output'] + '.parquet'
            else:
                outputFilename = add_filename_suffix(inputFilename, 'converted', 'parquet')
                
            # check delimeter argument
            if args['delimiter']:
                convert_csv_to_parquet(inputFilename, outputFilename,
                               delimiter=args['delimiter'])
            else:
                convert_csv_to_parquet(inputFilename, outputFilename)

            print_success_message(inputFilename, outputFilename, time_start)
    elif args['parquet2csv']:
        # convert parquet to csv
        if is_file_ext_correct('parquet2csv', args['parquet2csv'], 'parquet'):
            inputFilename = args['parquet2csv']
            
            # check output filename argument and convert
            if args['output']:
                outputFilename = args['output'] + '.csv'
            else:
                outputFilename = add_filename_suffix(inputFilename, 'converted', 'csv')

            # check delimeter argument and convert
            if args['delimiter']:
                convert_parquet_to_csv(inputFilename, outputFilename,
                               delimiter=args['delimiter'])
            else:
                convert_parquet_to_csv(inputFilename, outputFilename)

            print_success_message(inputFilename, outputFilename, time_start)
    elif args['get_schema']:
        # get schema of parquet
        if is_file_ext_correct('get_schema', args['get_schema'], 'parquet'):
            print(get_parquet_schema(args['get_schema']))
    else:
        # arguments are None
        print('Please, pass one of the necessary arguments for convertion:\n--csv2parquet\n--parquet2csv\n--get_schema \
            \n(example: --csv2parquet data.csv) \
            \n\nType --help for description of parameters.')


if __name__ == "__main__":
    main()
