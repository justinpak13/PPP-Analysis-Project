locals {
  data_lake_bucket = "ppp_data_lake"
}

variable "credentials" {
  description = "Where your auth keys are saved"
  default = "../keys/google_credentials.json"  # edit to match your filename 
}

variable "project" {
  description = "Your GCP Project ID"
  default = "ppp-project-344217" # Need to edit to specific project id 
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "us-east4"  # change based on your location 
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  type = string
  default = "ppp_data_all"
}