"""
Pipeline using GCP Client Libraries API to run ETL pipeline
from GS Bucket, apply transformations locally and write to
Big Query table.
"""

# Import packages for pipeline
import os
import sys
import re
from pathlib import Path
from shutil import rmtree
import json
import datetime

import pandas as pd

# Imports the Google Cloud client library
from google.cloud import storage
from google.cloud import bigquery

# Arg pars
# TODO: do argument parser for json

# Set up Google library authentication, follow steps:
# 1. Install and initialize the gcloud CLI. <link https://cloud.google.com/sdk/docs/install>
# 2. gcloud auth application-default login
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = f"{str(Path('~').expanduser())}/.config/gcloud/application_default_credentials.json" # See readme to update

# Set global variables for pipeline
# TODO: set in config
PROJECT_ID = 'assetinsure-surety-data-models'
BUCKET = 'surety-data-models'
GCS_INPUT = 'input'  # Cloud Storage bucket path location
GCS_OUTPUT = 'output' # Cloud Storage bucket path location
DATASET = "ls_panthers_test"
TABLE_NAME = "book"

def main():
    # Ensure we can see all packages in root of repo
    repo_root_dir = str(Path(".").resolve().parent)
    if repo_root_dir not in sys.path:
        sys.path.append(repo_root_dir)

    # Configure directories for local pipeline run
    base_dir = Path(sys.path[0])

    temp_e_dir = base_dir / "temp_e"
    temp_e_dir.mkdir(exist_ok=True, parents=True)

    temp_l_dir = base_dir / "temp_l"
    temp_l_dir.mkdir(exist_ok=True, parents=True)

    logs_dir = base_dir / "logs"
    logs_dir.mkdir(exist_ok=True, parents=True)

    # Instantiate clients
    storage_client_inst = storage.Client()
    bigquery_client_inst = bigquery.Client()

    # Download the file from GS to local temp_e and store file paths
    raw_file_paths = download_all_blobs(storage_client_inst, BUCKET, temp_e_dir, GCS_INPUT)
    # Log step
    pipeline_log(logs_dir, f"Loading these files locally: {raw_file_paths}", TABLE_NAME)

    # Create empty list for transformed dfs
    dfs_transformed = []

    # Read each file and apply transforms then concat
    # to dfs_transformed list
    for file in raw_file_paths:
        df_r = read_excel_file(file)
        df = apply_transforms(df_r)
        dfs_transformed.append(df)

    df_comb_bq = pd.concat(dfs_transformed)
    # Log step
    pipeline_log(logs_dir, f"Transformed data to df with shape: {df_comb_bq.shape}", TABLE_NAME)

    # Create local temp copy
    parquet_local(df_comb_bq, temp_l_dir)
    # Log step
    pipeline_log(logs_dir, f"DF to local: {temp_l_dir}/{TABLE_NAME}.parquet", TABLE_NAME)

    # Upload to GS Bucket transformed parquet data
    upload_blob(BUCKET, f"{temp_l_dir}/{TABLE_NAME}.parquet", f"{GCS_OUTPUT}/{TABLE_NAME}.parquet")
    # Log step
    pipeline_log(logs_dir, f"DF to GS: gs//{BUCKET}/{GCS_OUTPUT}/{TABLE_NAME}.parquet", TABLE_NAME)

    # Load parquet data from GS to Big Querty table
    load_parquet_to_bigquery(PROJECT_ID, BUCKET, GCS_OUTPUT, DATASET, TABLE_NAME)
    # Log step
    pipeline_log(logs_dir, f"Wrote to BQ successfully: {PROJECT_ID}.{DATASET}.{TABLE_NAME}", TABLE_NAME)

    # Log step
    pipeline_log(logs_dir, f"PIPELINE RAN SUCCESSFULLY - DELETE TEMP FILES IN DIRS {temp_e_dir} and {temp_l_dir}: ", TABLE_NAME)
    ## List directories to clean
    for path in [temp_e_dir, temp_l_dir]:
        cleanup(path)
    # Log step
    pipeline_log(logs_dir, f"PIPELINE END", TABLE_NAME)


# Logging function to write pipeline run steps to json
def pipeline_log(logs_dir, message, table_name):

    with open(f"{logs_dir}/{table_name}_logs.json", "a") as f:
        data = {
            "timestamp": str(datetime.datetime.today().strftime('%Y-%m-%d %H:%M:%S')),
            "logs": message
        }

        json.dump(data, f)
        f.write("\n")

# Download files from GS bucket
def download_all_blobs(storage_client, bucket_name, destination_directory, source_blob_prefix="", file_pattern=".*\.xlsm"):
    """
    Downloads all blobs from the bucket and saves them with their original file names.
    """

    # Initialise bucket
    bucket = storage_client.bucket(bucket_name)

    # List all blobs in the bucket with the specified prefix
    blobs = list(bucket.list_blobs(prefix=source_blob_prefix))

    local_file_paths = []

    for blob in blobs:
        # Don't download subfolder items
        if blob.name.endswith("/"):
            # Skip directories
            continue

        if re.search(file_pattern, blob.name.split("/")[-1]):

            # Extract file name
            file_name = blob.name.split("/")[-1]

            # Construct the local file path by joining the destination directory and relative path
            local_file_path = os.path.join(destination_directory, file_name)

            # Download the blob to the destination with its original name
            blob.download_to_filename(local_file_path)

            print("Downloaded storage object {} from bucket {} to local file {}.".format(
                blob.name, bucket_name, local_file_path))

            local_file_paths.append(local_file_path)

    return local_file_paths

# Read files and create pd DataFrame
def read_excel_file(file_path):
    try:
        # Attempt to read the Excel file
        df = pd.read_excel(file_path)
        return df
    except FileNotFoundError as e:
        # Log the filename and the error message
        print(f"Error: {e}. File not found: {file_path}")
        return None

# Cleaning and transformations
def transformation(df):
    # Get company name
    # first_non_null_index = df[df.iloc[:, 0].notnull()].index[0]
    # company_name = df.iloc[first_non_null_index, 0]
    # # Print company name
    # print(company_name)

    # # Get the current date and time
    # current_time = datetime.datetime.now()
    # # Print the current time
    # print(current_time)

    # # Create a dictionary to represent the row data
    # row_data = {
    #     'ID': [10,11,12],  # Replace with the actual ID if available
    #     'CompanyName': [company_name,company_name,company_name],
    #     'Date': [current_time,current_time,current_time]
    #     }

    # # Create dataframe of dictionary
    # df = pd.DataFrame.from_dict(row_data, orient='index').T

    # new test
    df["timestamp"] = datetime.datetime.now()

    return df

# Apply transformations to data
def apply_transforms(df_r):
    # TODO: Try except block
    df_t = transformation(df_r)
    # print(f"Apply transforms to file '{}'.")
    return df_t

# Local save of parquet file
def parquet_local(df, path):
    df.to_parquet(f"{path}/{TABLE_NAME}.parquet")

def upload_blob(storage_client, bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    # The details of GCS client
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    # Optional: set a generation-match precondition to avoid potential race conditions
    # and data corruptions. The request to upload is aborted if the object's
    # generation number does not match your precondition. For a destination
    # object that does not yet exist, set the if_generation_match precondition to 0.
    # If the destination object already exists in your bucket, set instead a
    # generation-match precondition using its generation number.

    # TODO: set the version in json config - to ovewrite pass None
    generation_match_precondition = 0

    blob.upload_from_filename(source_file_name, if_generation_match=generation_match_precondition)

    print(
        f"File {source_file_name} uploaded to {destination_blob_name}."
    )

def load_parquet_to_bigquery(bigquery_client, project_id, bucket_name, output_folder, dataset_id, table_id):
    # Construct transformed parquet file source URI of Google Storage object
    source_uri = f"gs://{bucket_name}/{output_folder}/{table_id}.parquet"

    # Construct BigQuery table reference
    table_id = f"{project_id}.{dataset_id}.{table_id}"

    # Load job config details
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE, # Options: WRITE_APPEND, WRITE_TRUNCATE, WRITE_EMPTY
        source_format=bigquery.SourceFormat.PARQUET

    )

    # Start the job to load from GS to BQ
    load_job = bigquery_client.load_table_from_uri(
        source_uri, table_id, job_config=job_config
    )

    print(f"Load data from GS location: {source_uri} to BQ Table ID: {load_job.destination}")

    # Wait for job to complete
    load_job.result()

    # Print job status
    # TODO: set logging
    print(f"Job ID: {load_job.job_id}")
    print(f"Job State: {load_job.state}")
    print(f"Loaded {load_job.output_rows} rows into {dataset_id}.{table_id} from {source_uri}")

def cleanup(clean_directory):
    # List all files to delete
    files = [f for f in clean_directory.glob("*")]
    for f in files:
        print(f"Deleting: {f}")

    try:
        rmtree(clean_directory)

    except Exception as e:
        print(f"An error occurred: {str(e)}")


if __name__ == "__main__":
    main()
