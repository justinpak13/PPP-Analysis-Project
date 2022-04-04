# Data Engineering Zoomcamp Final Project 
This repo contains the work carried out as part of the final project for the DE zoomcamp. This course is conducted by Alexey Grigorev. You can take the course at any time and it is free. Links to course mentioned below:
* https://github.com/DataTalksClub/data-engineering-zoomcamp
* https://www.youtube.com/c/DataTalksClub/playlists

## 1. About the Project
In response to the economic devastation of the COVID-19 pandemic, the United States Congress passed the Coronavirus Aid, Relief, and Economic Security (CARES) act. 

"The Paycheck Protection Program established by the CARES Act, is implemented by the Small Business Administration with support from the Department of the Treasury.  This program provides small businesses with funds to pay up to 8 weeks of payroll costs including benefits. Funds can also be used to pay interest on mortgages, rent, and utilities.

The Paycheck Protection Program prioritizes millions of Americans employed by small businesses by authorizing up to $659 billion toward job retention and certain other expenses." -> [Department of the Treasury](https://home.treasury.gov/policy-issues/coronavirus/assistance-for-small-businesses/paycheck-protection-program)

You can read more about the bill [here](https://www.congress.gov/bill/116th-congress/house-bill/748)

This project aims to create a pipeline using a mix of technologies to get the data from the SBA website, make the appropriate transformations, upload to Google Cloud Service, and finally present the data in a dashboard using Google Data Studio

**Data set**: https://data.sba.gov/dataset/ppp-foia

**NAICS Codes**: https://www.census.gov/naics/?48967

 
## 2. Process

I will be using GCP cloud storage as the data lake and Airflow in order to get the data into GCP. 

The data workflow will consist of the following steps:
1) Set up GCP and create our project 
2) Use terraform to create our infrastructure in GCP 
3) Using Airflow through Docker to run our pipeline:

Pipeline:
1) Downloading the csv files from the SBA website 
2) Use Pyspark in order to make necessary transformations to our CSV files and save as Parquet
3) Upload the parquet files into Google Cloud Storage 
4) Upload our files in Google Cloud Storage into a table in BigQuery 
5) Take our Bigquery data and create a dashboard in Google data studio 

Requirements: 
* GCP Account
* Terraform
* Docker

## 3. Setting up GCP 

After you have cloned the repo, we first need to set up GCP:
* Create an account with Google email 
* Set up the project in the google cloud console
* setup service account and authentication for this project and download auth-keys
    * go to IAM & Admin -> service accounts -> create service account
    * create access to within the permissions tab
        * storage admin
        * storage object admin 
        * BigQuery admin
    * Under APIs and Services, enable the following:  
        * Identity and Access Management (IAM) API
        * IAM service account credentials API
    * back under the IAM & admin section, select the service account you made earlier. Then go to keys -> add key -> create new json key
    * In the folder that you saved this repo in, there should be a keys folder. In the keys folder, replace the text file with your json file containing your key and rename the file "google_credentials.json" for consistency going forward
* download SDK for local setup 
    * https://cloud.google.com/sdk
* set up environment variable to point to your downloaded auth-keys. Run the following code but replace the path

    <code> export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>google_credentials.json"</code> use set instead of export on windows 

* Refresh token/session, and verify authentication

    <code>gcloud auth application-default login</code>

 
## 4. Terraform 

https://www.terraform.io/downloads

**Creating GCP Infrastructure with Terraform**

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

 Do not change anything in the main file. All changes should be made in the variables file 

* variables.tf: 
    * This file is where you specify the instances in the main file that use "var". In our case, the necessary ones are the project id and the region. The project id can be found in the main console page for your project while the region is based on your location. 
    * locals: similar to constants 
    * vairables: 
        * generally passed during runtime
        * can have default values 
    * change all information that have comments to what would be applicable for you 

   
**Execution Steps**

Once the files are established, you can run these commands within the terraform folder (except for destroy)
* <code>terraform init</code>: Initialize and Install
* <code>terraform plan</code>: Match changes against the previous state
* <code>terraform apply</code>: Apply changes to cloud 
* <code>terraform destroy</code>: Remove your stack from the cloud 

GCS is now ready for the data pipeline 
 
## 5. Airflow 

We will be using Apache Airflow through Docker, so make sure you have Docker downloaded and working.
Also, I started this project using an M1 Macbook and had a terrible time getting docker running, so I had to switch over to a windows pc. YMMV.

In docker, make sure that everything is up to date and that you have the correct amount of ram allocated (5gb min, ideally 8)

### Setup 
I created an airflow folder and within the folder, imported the official image and setup form the latest airflow version. This downloaded the the docker-compose.yaml file in the folder 

DO NOT RUN
<code>curl -LfO 'https://airflow.apache.org/docs/apache-airflow/2.2.4/docker-compose.yaml'</code>

Next we need to set the aiflow user and create the logs, plugins, and scripts folders, I have already created the dags folder.
 
Within the aiflow folder either run the following commands to create the necessary folders and .env file or create them manually.

<code>mkdir -p ./logs ./plugins ./scripts</code>
 
<code>echo -e "AIRFLOW_UID=$(id -u)" > .env</code>

 
* The dags directory is where you will be storing the airflow pipelines 
* The logs directory stores the log information between the scheduler and the workers 
* The plugins folder stores any custom function that are used within your dags 

In order to ensure that airflow will work with GCP, we will create a custom Dockerfile and take the base image from our docker-compose.yaml file. The necessary google requirements will then be installed through the Dockerfile. (https://airflow.apache.org/docs/docker-stack/recipes.html) We then connect our docker-compose file to our Dockerfile by replacing the image with build. 

Our Dockerfile essentially downloads the google cli tools and the requirements for PySpark and saves into our path, sets our user, and then imports the python packages from a saved requirements file

The only things that need to be edited in the docker-compose.yaml file are the GCP_PROJECT_ID (can be found on homepage dashboard), the GCP_GCS_BUCKET (found in cloud storage section) and the BIGQUERY_DATASET (what you named the dataset in terraform. Can be found in bigquery but if you did not edit it, then it would be ppp_data_all). Please change lines 65-67 based on your configuration of GCS
 
(While we are editing the docker-compose.yaml file, also change the AIRFLOW__CORE__LOAD_EXAMPLES to false or else it will load predefined dag examples.)
 
 Then run the following commands: 
 
 <code>docker-compose build</code> this may take a while 
 
 <code>docker-compose up airflow-init</code>
 
 <code>docker-compose up</code> 
 
 Then run this command in another terminal to make sure everything is running healthy 
 <code>docker ps</code>
 
 

## 6. Aiflow DAGS
 
Once everything is running, you can navigate to http://localhost:8080/ and trigger the DAG. The username and password is airflow. Go get some coffee as this will probably take a while. The section below explains what is happening behind the scenes 

 The  DAG that is outlined below is not the most efficient. Since there are 13 large csv files to be downloaded from the SBA website, within our DAG file I created a list with the links. Then I set the start date to be 13 days prior to the current date of the run. Once Airflow runs, it counts the difference in days between the run date and the start date and chooses the corresponding link in the list. The approach is pretty roundabout, but I have created it this way for a few reasons. One, in order to resemble how more realistic batch data processing would work. Two, it allowed more immediate feedback when working on the project so I could move on to other sections of the DAG without waiting for all 13 files to be downloaded at once. And 3, it was an excuse to learn how to have different parts of the DAG communicate with each other. 
 
The DAG we are using is broken down into 6 different steps:
1) wget_task: This task counts the difference in days between the run date and the current date. This number will correspond with one of the 13 links in the next task
2) choose_link: This task takes in the value generated by the previous task and returns the corresponding link to the next task 
3) download_dataset_task: This task takes the link given by the previous task and proceeds to download the CSV file 
4) format_to_parquet_task: This task uses pyspark in order to read the downloaded csv file, make the necessary transformations, and save the file as multiple parquet files. We will go into more detail in the next section
5) local_to_gcs_task: This task takes the saved parquet files and then uplaods them to our data lake in google cloud storage 
6) bigquery_external_table_task: This task takes the data in google cloud storage and uploads them to our data warehouse in BigQuery 

## 7. PySpark 
 
The format_to_parquet_task in our Dag contains our PySpark Code. This function first creates a SparkSession. Then it applies a schema which I have created on the saved csv to create a pyspark dataframe. It then does the same thing to our predownloaded csv file that contains our NAICS codes such that they have no issues when merging. The dates are also transformed so that pyspark can more easily read them as datetime types. The zip codes in the data are not uniform in the way that they are read in, so a transformation is done so that google data studio can more easily read them in as postal code types. Finally, it drops some unecessary columns and saves down in a folder of parquet files
 
## 8. Google Data Studio Dashboard
I created my dashboard within google data studio by connecting to the datawarehouse we created in airflow to a report. This should be fairly straighforward if you are using the same google account for both. I also made the necessary schema adjustments to properly match the data to the future charts. The dashboard is broken up into two pages. The first being an overview with a breakdown based on different categories and the second as a map with a table with information based on zip code. Sorry that it isnt that pretty. 
 
Link to the dashboard [here](https://datastudio.google.com/reporting/382aebdd-ae5d-4683-ab4c-2e81357e7295)
 


