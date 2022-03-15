# data-enineering-zoomcamp-project
Analysis of PPP loan data for the final project of the Data Engineering Zoomcamp

In response to the economic devastation of the COVID-19 pandemic, the United States Congress passed the Coronavirus Aid, Relief, and Economic Security (CARES) act. 

"The Paycheck Protection Program established by the CARES Act, is implemented by the Small Business Administration with support from the Department of the Treasury.  This program provides small businesses with funds to pay up to 8 weeks of payroll costs including benefits. Funds can also be used to pay interest on mortgages, rent, and utilities.

The Paycheck Protection Program prioritizes millions of Americans employed by small businesses by authorizing up to $659 billion toward job retention and certain other expenses." -> [Department of the Treasury](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-small-businesses/paycheck-protection-program)

<b>Data set</b> : https://data.sba.gov/dataset/ppp-foia

<b>Question</b>:
* General facts of the program
* What do the distributions of the funds look like compared to the makeup of [small businesses](https://cdn.advocacy.sba.gov/wp-content/uploads/2019/04/23142719/2019-Small-Business-Profiles-US.pdf)
 
# Process

## Creating a pipeline for processing this dataset & putting it into a data lake 

I will be using GCP cloud storage as the data lake and Airflow in order to get the data into GCP. 

The data workflow will consist of the following steps: 
1) Downloading the csv files from the SBA website that we will store locally 
2) Use a python script in order to convert the csv files into parquet 
3) Upload the parquet files into Google Cloud Storage 
4) Upload our files in Google Cloud Storage into a table BigQuery 

### Requirements

#### Terraform 

https://www.terraform.io/downloads

#### Setting up GCP 
Before working with our data, we first need to set up GCP:
* Create an account with Google email 
* Set up the project 
* setup service account and authentication for this project and download auth-keys
    * go to IAM & Admin -> service accounts -> create service account
    * grant accesss
    * go to manage keys -> create new json key
    * save to key folder in project 
* download SDK for local setup 
    * https://cloud.google.com/sdk
* set up environment variable to point to your downloaded auth-keys 

    <code> export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"</code>

    Refresh token/session, and verify authentication

    <code>gcloud auth application-default login</code>

* create access to 
    * storage admin
    * storage object admin 
    * BigQuery admin
* Under APIs and Services, enable the following:  
    * Identity and Access Management (IAM) API
    * IAM service account credentials API
 

#### Creating GCP Infrastructure with Terraform

Within the terraform folder there are 3 files:
* .terraform-version: just has the version number of terraform 
* main.tf:
    * terraform 
        * this section of the code declares the terraform version, the backend (whether local, gcs, or s3), and the required providers which specifies public libraries where we will be getting functions from (kind of like python libraries and pip)
    * provider
        * adds a set of predefined resource types and data sources that terraform can manage such as google cloud storage bucket, data lake bucket, bigquery dataset, etc 
        * where the variable for credentials would go
    * resource 
        * Specify and configure the resources 

    * Do not change anything in this file. All changes should be made in the variables file 

* variables.tf: 
    * This file is where you specify the instances in the main file that use "var"
    * locals: similar to constants 
    * vairables: 
        * generally passed during runtime
        * can have default values 
    * change all information that have comments to what would be applicable for you 

    

**Execution Steps**

Once the files are established, you can run these commands within the folder (except for destroy)
* <code>terraform init</code>: Initialize and Install
* <code>terraform plan</code>: Match changes against the previous state
* <code>terraform apply</code>: Apply changes to cloud 
* <code>terraform destroy</code>: Remove your stack from the cloud 

 
