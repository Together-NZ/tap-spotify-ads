resource "google_bigquery_dataset" "dash_union__centralized" {
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  access {
    role          = "OWNER"
    user_by_email = "peter@wearetogether.co.nz"
  }

  access {
    role          = "READER"
    special_group = "projectReaders"
  }

  access {
    role          = "WRITER"
    special_group = "projectWriters"
  }

  dataset_id                 = "dash_union__centralized"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "US"
  max_time_travel_hours = "168"
  project               = "amp-main"
}
# terraform import google_bigquery_dataset.dash_union__centralized projects/amp-main/datasets/dash_union__centralized
