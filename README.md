# surity-pipeline-etl-api
Personal repo for surity pipeline development and testing for the API version


## GCP API and BQ API
Create pipeline version calling apis fro GS and BQ to clean excel file and write parquet.


## Set up authentication for client libraries
User credentials
When your code is running in a local development environment, such as a development workstation, the best option is to use credentials associated with your Google Account, also called user credentials.

To provide your user credentials to ADC, you use the Google Cloud CLI:

Install and initialize the gcloud CLI.

Create your credential file:


gcloud auth application-default login
A login screen is displayed. After you log in, your credentials are stored in the local credential file used by ADC.

You can provide user credentials to ADC by running the gcloud auth application-default login command. This command places a JSON file containing the credentials you provide (usually from your own Google Account) in a well-known location on your file system. The location depends on your operating system:

Linux, macOS: $HOME/.config/gcloud/application_default_credentials.json
Windows: %APPDATA%\gcloud\application_default_credentials.json
