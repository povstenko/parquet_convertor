# import the necessary packages
import pandas as pd
import csv
import json
import argparse
import pyarrow

# define functions for convert
def csv_to_parquet(csv_path: str, parquet_path: str, delimiter=','):
    """Сonvert csv to parquet and save to file"""
    df = pd.read_csv(csv_path, sep=delimiter)
    df.to_parquet(parquet_path)


def parquet_to_csv(parquet_path: str, csv_path: str, delimiter=','):
    """Сonvert parquet to csv and save to file"""
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path, sep=delimiter, index=False)


def csv_to_json(csv_path: str, json_path: str, json_indent=4):
    """Сonvert csv to json and save to file"""
    data = []

    # read csv file
    with open(csv_path) as csvFile:
        csvReader = csv.DictReader(csvFile)
        data = [row for row in csvReader]

    # write json
    with open(json_path, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent=json_indent))


def parquet_schema(parquet_path: str):
    """Get schema of parquet file"""
    df = pd.read_parquet(parquet_path)
    print(pyarrow.Table.from_pandas(df=df).schema)


# define functions for working with filenames
def add_filename_suffix(filename: str, suffix: str, extension: str) -> str:
    """Add suffix for filename and change extension"""
    return filename.split('.')[0] + '_' + suffix + '.' + extension


def is_file_ext_correct(parameter: str, filename: str, extension: str) -> bool:
    """Returns True if filename has correct file extension and prints message otherwise"""
    if filename.split('.')[1] != extension:
        print(
            f'Wrong argument for --{parameter}. You must specify *.{extension} file for input')
        return False
    else:
        return True


def main():
    # construct the argument parser and parse the arguments
    ap = argparse.ArgumentParser(
        description='Script for converting CSV, PARQUET and JSON files.')
    ap.add_argument("-cp", "--csv2parquet", type=str,
                    help="Convert csv to parquet. Set input csv filename string (example: data.csv)")
    ap.add_argument("-pc", "--parquet2csv", type=str,
                    help="Convert parquet to csv. Set input parquet filename string (example: data.parquet)")
    ap.add_argument("-cj", "--csv2json", type=str,
                    help="Convert csv to json. Set input csv filename string (example: data.csv)")
    ap.add_argument("-s", "--get_schema", type=str,
                    help="Get schema of parquet file. Set input parquet filename string (example: data.parquet)")
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
            outputFilename = add_filename_suffix(inputFilename, 'converted', 'json')

            csv_to_json(inputFilename, outputFilename,
                        json_indent=args['json_indent'])

            print(f'Successfully converted from {inputFilename} to {outputFilename}')
    elif args['get_schema']:
        # get schema of parquet
        if is_file_ext_correct('get_schema', args['get_schema'], 'parquet'):
            parquet_schema(args['get_schema'])
    else:
        print('Please, pass the necessary arguments for convertion (example: --csv2parquet data.csv).')
        print('Type --help for description of parameters.')


if __name__ == "__main__":
    main()
