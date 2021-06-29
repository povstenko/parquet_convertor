"""Data Files Convertor

This script allows the user to convert different data types. 

This tool accepts comma separated value files (.csv) as well as apache parquet
(.parquet) files. It is assumed that the first row of the spreadsheet is the
location of the columns.

This script requires that `pandas`, `argparse`, `pyarrow`, `csv` and `json` be installed within the Python
environment you are running this script in.

This file can also be imported as a module and contains the following
functions:

    * csv_to_parquet - convert csv to parquet and save to file
    * parquet_to_csv - convert parquet to csv and save to file
    * csv_to_json - convert csv to json and save to file
    * parquet_schema - returns schema of parquet file
    * add_filename_suffix - returns filename string with added suffix for filename and change extension
    * is_file_ext_correct - returns returns True if filename has correct file extension and prints message otherwise
    * main - the main function of the script
"""

# import the necessary packages
import pandas as pd
import csv
import json
import argparse
import pyarrow

# define functions for convert
def csv_to_parquet(csv_path: str, parquet_path: str, delimiter=','):
    """Сonvert csv to parquet and save to file
    
    Parameters
    ----------
    csv_path: str
        The file name of the csv
    parquet_path: str
        The file name of the parquet
    delimiter: str, optional
        Delimiter to use in parsing engine (default is ',')
    """
    df = pd.read_csv(csv_path, sep=delimiter)
    df.to_parquet(parquet_path)


def parquet_to_csv(parquet_path: str, csv_path: str, delimiter=','):
    """Сonvert parquet to csv and save to file
    
    Parameters
    ----------
    parquet_path: str
        The file name of the parquet
    csv_path: str
        The file name of the csv
    delimiter: str, optional
        Delimiter to use in parsing engine (default is ',')
    """
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, sep=delimiter, index=False)


def csv_to_json(csv_path: str, json_path: str, json_indent=4):
    """Сonvert csv to json and save to file
    
    Parameters
    ----------
    csv_path: str
        The file name of the csv
    json_path: str
        The file name of the json
    json_indent: int, optional
        Indent for json output file (default is 4)
    """
    data = []

    # read csv file
    with open(csv_path) as csvFile:
        csvReader = csv.DictReader(csvFile)
        data = [row for row in csvReader]

    # write json
    with open(json_path, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent=json_indent))


def parquet_schema(parquet_path: str) -> str:
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
    df = pd.read_parquet(parquet_path)
    return pyarrow.Table.from_pandas(df=df).schema


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
    return filename.split('.')[0] + '_' + suffix + '.' + extension


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
    if filename.split('.')[1] != extension:
        print(
            f'Wrong argument for --{parameter}. You must specify *.{extension} file for input')
        return False
    else:
        return True


def main():
    # construct the argument parser and parse the arguments
    ap = argparse.ArgumentParser(
        description=__doc__)
    ap.add_argument("-cp", "--csv2parquet", type=str,
                    help="Convert csv to parquet. Set input csv filename string (example: data.csv)")
    ap.add_argument("-pc", "--parquet2csv", type=str,
                    help="Convert parquet to csv. Set input parquet filename string (example: data.parquet)")
    ap.add_argument("-cj", "--csv2json", type=str,
                    help="Convert csv to json. Set input csv filename string (example: data.csv)")
    ap.add_argument("-s", "--get_schema", type=str,
                    help="Get schema of parquet file. Set input parquet filename string (example: data.parquet)")
    ap.add_argument("-o", "--output", type=str,
                    help="Set output file name without extension (example: newfile)")
    ap.add_argument("-d", "--delimiter", type=str, default=",",
                    help="Set delimiter for csv file (default: ,)")
    ap.add_argument("-i", "--json_indent", type=int,
                    help="Set indent for json file (default: 4)")
    args = vars(ap.parse_args())

    # check convert option
    if args['csv2parquet']:
        # convert csv to parquet
        if is_file_ext_correct('csv2parquet', args['csv2parquet'], 'csv'):
            inputFilename = args['csv2parquet']
            
            if args['output']:
                outputFilename = args['output'] + '.parquet'
            else:
                outputFilename = add_filename_suffix(inputFilename, 'converted', 'parquet')

            if args['delimiter']:
                csv_to_parquet(inputFilename, outputFilename,
                               delimiter=args['delimiter'])
            else:
                csv_to_parquet(inputFilename, outputFilename)

            print(f'Successfully converted from {inputFilename} to {outputFilename}')
    elif args['parquet2csv']:
        # convert parquet to csv
        if is_file_ext_correct('parquet2csv', args['parquet2csv'], 'parquet'):
            inputFilename = args['parquet2csv']
            
            if args['output']:
                outputFilename = args['output'] + '.csv'
            else:
                outputFilename = add_filename_suffix(inputFilename, 'converted', 'csv')

            if args['delimiter']:
                parquet_to_csv(inputFilename, outputFilename,
                               delimiter=args['delimiter'])
            else:
                parquet_to_csv(inputFilename, outputFilename)

            print(f'Successfully converted from {inputFilename} to {outputFilename}')
    elif args['csv2json']:
        # convert csv to json
        if is_file_ext_correct('csv2json', args['csv2json'], 'csv'):
            inputFilename = args['csv2json']
            
            if args['output']:
                outputFilename = args['output'] + '.json'
            else:
                outputFilename = add_filename_suffix(inputFilename, 'converted', 'json')
            
            csv_to_json(inputFilename, outputFilename,
                        json_indent=args['json_indent'])

            print(f'Successfully converted from {inputFilename} to {outputFilename}')
    elif args['get_schema']:
        # get schema of parquet
        if is_file_ext_correct('get_schema', args['get_schema'], 'parquet'):
            print(parquet_schema(args['get_schema']))
    else:
        print('Please, pass the necessary arguments for convertion (example: --csv2parquet data.csv).')
        print('Type --help for description of parameters.')


if __name__ == "__main__":
    main()
