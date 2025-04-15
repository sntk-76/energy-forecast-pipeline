variable "project_id" {
    description = "The project id "
    default = "energy-forecast-pipeline"
}

variable "service_account" {
    description = "The service account json file directory for the gcp connection"
    default = "/home/sinatavakoli284/energy-forecast-pipeline/authentication/gcp-sa-key.json"
}

variable "region" {
    description = "The region of the project"
    default = "europe-west8"
}

variable "bucket_name" {
    description = "The name of the bucket forthe project that contain three folders"
    default = "energy-forecast-pipeline_bucket"
}

variable "bigquery_name_1" {
    description = "The name of the data set for the clean data after transformation"
    default = "cleaned_data"
}

variable "bigquery_name_2" {
    description = "The name of the data set for the forcast process "
    default = "forcast_data"
}

variable "bigquery_name_3" {
    description = "The name of the data set for the staging process "
    default = "staging"
}

variable "bigquery_name_4" {
    description = "The name of the data set for the marts process "
    default = "mart"
}

