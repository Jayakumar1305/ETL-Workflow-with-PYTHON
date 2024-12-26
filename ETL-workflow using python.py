import os
import pandas as pd
import xml.etree.ElementTree as ET
from datetime import datetime
import glob
import requests
import zipfile

# Step 1: Download the dataset using requests
url = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip"
zip_file_path = "source.zip"

# Download the file
response = requests.get(url)
with open(zip_file_path, 'wb') as file:
    file.write(response.content)

print(f"Downloaded {zip_file_path}.")

# Step 2: Unzip the downloaded file
unzipped_folder_path = 'C:/temp/users/unzipped_folder'  # Change this to an accessible path

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(unzipped_folder_path)

# Step 1: Set up paths
log_dir = 'C:/temp/users/log'
output_file = 'C:/temp/users/txn'
os.makedirs(log_dir, exist_ok=True)  # Ensure the directory exists
os.makedirs(output_file, exist_ok=True)  # Ensure the directory exists
log_file_path = os.path.join(log_dir, "etllog.csv")
output_file_path = os.path.join(output_file, "transformed_df.csv")

# Logging function
def log_message(message):
    """Logs a message with a timestamp to the log file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(log_file_path, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

# Step 2: Extract functions
# Extract from CSV
def extract_from_csv(file_path):
    """Extract data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        log_message(f"Extracted data from CSV: {file_path}")
        return df
    except Exception as e:
        log_message(f"Error extracting from CSV: {file_path}, {e}")
        return pd.DataFrame()


# Extract from JSON
def extract_from_json(file_path):
    """Extract data from a JSON file."""
    try:
        df = pd.read_json(file_path, lines=True)
        log_message(f"Extracted data from JSON: {file_path}")
        return df
    except Exception as e:
        log_message(f"Error extracting from JSON: {file_path}, {e}")
        return pd.DataFrame()


# Extract from XML
def extract_from_xml(file_path):
    """Extract data from an XML file."""
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        records = []
        for child in root:
            record = {elem.tag: elem.text for elem in child}
            records.append(record)
        df = pd.DataFrame(records)
        log_message(f"Extracted data from XML: {file_path}")
        return df
    except Exception as e:
        log_message(f"Error extracting from XML: {file_path}, {e}")
        return pd.DataFrame()

# Master Extract Function
def extract_data(file_paths):
    """Extract data from all file types and combine into a single DataFrame."""
    combined_df = pd.DataFrame()
    for file_path in file_paths:
        _, ext = os.path.splitext(file_path)

        if ext.lower() == ".csv":
            df = extract_from_csv(file_path)

        elif ext.lower() == ".json":
            df = extract_from_json(file_path)

        elif ext.lower() == ".xml":
            df = extract_from_xml(file_path)
        else:
            log_message(f"Unsupported file type: {file_path}")
            continue
        combined_df = pd.concat([combined_df, df], ignore_index=True)
    log_message("Data extraction complete.")
    return combined_df

# Step 3: Transform data
def transform_data(df):
    """Transform height from inches to meters and weight from pounds to kilograms."""
    try:
        df["height"] = df["height"].astype(float) * 0.0254  # inches to meters
        df["weight"] = df["weight"].astype(float) * 0.453592  # pounds to kg
        log_message("Data transformation complete.")
        return df
    except Exception as e:
        log_message(f"Error during data transformation: {e}")
        return pd.DataFrame()

# Step 4: Load data
def load_data(df, output_path):
    """Save the transformed data to a CSV file."""
    try:
        df.to_csv(output_path, index=False)
        log_message(f"Data successfully saved to {output_path}")
    except Exception as e:
        log_message(f"Error saving data: {e}")

# ETL Execution
if __name__ == "__main__":
    log_message("ETL process started.")

    # File Paths
    file_paths = glob.glob(os.path.join(unzipped_folder_path, "*"))

    # Extraction Phase
    log_message("Extraction phase started.")
    extracted_df = extract_data(file_paths)

    # Transformation Phase
    log_message("Transformation phase started.")
    transformed_df = transform_data(extracted_df)

    # Loading Phase
    log_message("Loading phase started.")
    load_data(transformed_df, output_file_path)

    log_message("ETL process completed.")
