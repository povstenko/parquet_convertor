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

print(args)

def csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path)

def pd_parquet_to_csv(parquet_path: str, csv_path: str):
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path)
    
    return df

def csv_to_json(csv_path:str, json_path:str, json_indent=4):
    data = {}
    
    with open(csv_path) as csvFile:
        csvReader = csv.DictReader(csvFile)
        for rows in csvReader:
            id = rows['id']
            data[id] = rows
    
    with open(json_path, 'w') as jsonFile:
        jsonFile.write(json.dumps(data, indent=json_indent))

def main():
    
    if args['csv2parquet']:
        # convert csv to parquet
        inputFilename = args['csv2parquet']
        p = Path(inputFilename)
        outputFilename = p.with_suffix('.parquet')
        
        csv_to_parquet(inputFilename, outputFilename)
        print(f'Successfully converted from {inputFilename} to {outputFilename}')
        
    elif args['parquet2csv']:
        print('b')
    
    # pd_csv_to_parquet('data.csv', 'data.parquet')
    # pd_parquet_to_csv('data.parquet', 'data_conv.csv')
    # csv_to_json('data.csv','data.json')

if __name__ == "__main__":
    main()