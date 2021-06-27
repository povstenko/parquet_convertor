import pandas as pd

def pd_csv_to_parquet(csv_path: str, parquet_path: str):
    df = pd.read_csv(csv_path)
    df.to_parquet(parquet_path)
    
    return df

def pd_parquet_to_csv(parquet_path: str, csv_path: str):
    df = pd.read_parquet(parquet_path)
    df.to_csv(csv_path)
    
    return df

def main():
    pd_csv_to_parquet('data.csv', 'data.parquet')
    pd_parquet_to_csv('data.parquet', 'data_conv.csv')

if __name__ == "__main__":
    main()