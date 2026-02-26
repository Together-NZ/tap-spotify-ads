resource "google_bigquery_dataset" "google_ads" {
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "OWNER"
    user_by_email = "developer@wearetogether.co.nz"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  access {
    role          = "WRITER"
    user_by_email = "service-364275589560@gcp-sa-bigquerydatatransfer.iam.gserviceaccount.com"
  }

  dataset_id                 = "google_ads"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "amp-main"
}
# terraform import google_bigquery_dataset.google_ads projects/amp-main/datasets/google_ads
