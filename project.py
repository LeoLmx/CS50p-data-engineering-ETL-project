import glob
import pandas as pd
from datetime import datetime


def main():
    log("ETL Job Started")

    log("Extract phase Started")
    extracted_data = extract()
    log("Extract phase Ended")

    log("Transform phase Started")
    transform_data = transform(extracted_data)
    log("Transform phase Ended")

    log("Load phase Started")
    print(load(transform_data))
    log("Load phase Ended")


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


def extract_from_csv():
    df = pd.read_csv("/Users/leorickli/Documents/Python/CS50p/project/exchange_rates.csv", names=['Country', 'Rates'], skiprows=1)
    csv_file = df.loc[9]['Rates']
    return csv_file


def extract():
    extracted_data = pd.DataFrame(columns=['Name','Market Cap (US$ Billion)'])
    all_files = glob.glob("*.json")
    extracted_data = pd.concat((extract_from_json(f) for f in all_files), ignore_index=True)
    return extracted_data


def transform(data):
    exchange_rate = extract_from_csv()
    data["Market Cap (US$ Billion)"]  = data["Market Cap (US$ Billion)"] * exchange_rate
    data["Market Cap (US$ Billion)"] = data["Market Cap (US$ Billion)"].round(3)
    data.rename(columns={"Market Cap (US$ Billion)": "Market Cap (GBP$ Billion)"}, inplace = True)
    return data


def load(data):
    data.to_csv('bank_market_cap_gbp.csv', index=False)
    return "Success"
    
    
def log(message):
    timestamp_format = '%Y-%h-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open("logfile.txt","a") as f:
        f.write(timestamp + ',' + message + '\n')
        
        
if __name__ == "__main__":
    main()