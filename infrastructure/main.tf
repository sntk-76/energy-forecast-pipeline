provider "google" {
    project = var.project_id
    credentials = file(var.service_account)
    region = var.region
}

resource "google_storage_bucket" "gcp_bucket" {

    name = var.bucket_name
    location = var.region
    project = var.project_id
    force_destroy = true
}



resource "google_bigquery_dataset" "clean_gcp_bigquery" {

    dataset_id = var.bigquery_name_1
    project = var.project_id
    location = var.region
  
}

resource "google_bigquery_dataset" "forecast_gcp_bigquery" {

    dataset_id = var.bigquery_name_2
    project = var.project_id
    location = var.region
  
}

