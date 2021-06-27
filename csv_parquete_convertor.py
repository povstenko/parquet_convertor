import pandas as pd
import csv, json

def pd_csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path)
    
    return df

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
    pd_csv_to_parquet('data.csv', 'data.parquet')
    pd_parquet_to_csv('data.parquet', 'data_conv.csv')
    csv_to_json('data.csv','data.json')

if __name__ == "__main__":
    main()