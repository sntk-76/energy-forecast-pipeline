variable "project_id" {
    description = "The project id "
    default = "energy-forecast-pipeline"
}

variable "service_account" {
    description = "The service account json file directory for the gcp connection"
    default = "/home/sinatavakoli284/energy-forecast-pipeline/Authentication/energy-forecast-pipeline-4005547e63fc.json"
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

