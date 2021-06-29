# import the necessary packages
import pandas as pd
import csv, json
import argparse
from pathlib import Path
 
# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-cp", "--csv2parquet", required=False, help="Convert csv to parquet")
ap.add_argument("-pc", "--parquet2csv", required=False, help="Convert parquet to csv")
ap.add_argument("-cj", "--csv2json", required=False, help="Convert csv to json")
ap.add_argument("-s", "--get-schema", required=False, help="Get schema of parquet file")
args = vars(ap.parse_args())

# print(args)

def csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path)

def parquet_to_csv(parquet_path: str, csv_path: str):
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path)

def csv_to_json(csv_path:str, json_path:str, json_indent=4):
    data = []
    
    with open(csv_path) as csvFile:
        csvReader = csv.DictReader(csvFile)
        data = [row for row in csvReader]

    with open(json_path, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent=json_indent))

def main():
    
    if args['csv2parquet']:
        # convert csv to parquet
        
        inputFilename = args['csv2parquet']
        if inputFilename.split('.')[1] != 'csv':
            print('Wrong argument for --csv2parquet. You must specify *.csv file for input')
        else:
            outputFilename = inputFilename.split('.')[0] + "_converted.parquet"
            csv_to_parquet(inputFilename, outputFilename)
            print(f'Successfully converted from {inputFilename} to {outputFilename}')
        
    elif args['parquet2csv']:
        # convert parquet to csv
        
        inputFilename = args['parquet2csv']
        if inputFilename.split('.')[1] != 'parquet':
            print('Wrong argument for --parquet2csv. You must specify *.parquet file for input')
        else:
            outputFilename = inputFilename.split('.')[0] + "_converted.csv"
            parquet_to_csv(inputFilename, outputFilename)
            print(f'Successfully converted from {inputFilename} to {outputFilename}')
            
    elif args['csv2json']:
        # convert csv to json
        
        inputFilename = args['csv2json']
        if inputFilename.split('.')[1] != 'csv':
            print('Wrong argument for --csv2json. You must specify *.csv file for input')
        else:
            outputFilename = inputFilename.split('.')[0] + "_converted.json"
            csv_to_json(inputFilename, outputFilename)
            print(f'Successfully converted from {inputFilename} to {outputFilename}')
    else:
        print('Please, pass the necessary arguments for convertion (For example: --csv2parquet data.csv).\nType --help for description of arguments.')

if __name__ == "__main__":
    main()