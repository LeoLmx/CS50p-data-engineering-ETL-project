import glob
import pandas as pd
from datetime import datetime


def main():
    log("ETL Job Started")

    log("Extract phase Started")
    extracted_data = extract()
    log("Extract phase Ended")

    log("Transform phase Started")
    transformed_data = transform(extracted_data)
    log("Transform phase Ended")

    log("Load phase Started")
    print(load(transformed_data))
    log("Load phase Ended")


def extract_from_json(file_to_process):
    dataframe = pd.read_json(file_to_process)
    return dataframe


def extract_from_csv():
    df = pd.read_csv("exchange_rates.csv", names=['Country', 'Rates'], skiprows=1)
    # Get the GCP exchange rate on line 10, column "Rates"
    csv_file = df.loc[9]['Rates']
    return csv_file


def extract():
    # Create an empty dataframe to store results (NOTE: this is currently unused)
    pd.DataFrame(columns=['Name', 'Market Cap (US$ Billion)'])

    # Get all JSON files in the directory
    all_files = glob.glob("*.json")

    # Read each JSON file into a dataframe and combine them
    extracted_data = pd.concat((extract_from_json(f) for f in all_files), ignore_index=True)

    return extracted_data


def transform(extracted_data):
    # Get the exchange rate for GBP from the CSV
    exchange_rate = extract_from_csv()

    # Convert USD to GBP and round the results
    extracted_data["Market Cap (US$ Billion)"] *= exchange_rate
    extracted_data["Market Cap (US$ Billion)"] = extracted_data["Market Cap (US$ Billion)"].round(3)

    # Rename the market cap column to GBP
    transformed_data = extracted_data.rename(columns={"Market Cap (US$ Billion)": "Market Cap (GBP$ Billion)"})

    return transformed_data


def load(transformed_data):
    # Save the transformed data to a CSV file
    transformed_data.to_csv('bank_market_cap_gbp.csv', index=False)
    return "Success"


def log(message):
    # Function to log messages with timestamps
    timestamp_format = '%Y-%h-%d-%H:%M:%S'  # Format for timestamp
    now = datetime.now()  # Get the current datetime
    timestamp = now.strftime(timestamp_format)  # Format as string
    with open("logfile.txt", "a") as f:  # Open logfile in append mode
        f.write(timestamp + ',' + message + '\n')  # Write log entry


if __name__ == "__main__":
    main()
