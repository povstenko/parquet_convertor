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
    df = pd_csv_to_parquet('data.csv', 'data.parquet')
    df = pd_parquet_to_csv('data.parquet', 'data.csv')
    print(df)

if __name__ == "__main__":
    main()