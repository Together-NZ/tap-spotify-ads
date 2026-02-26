resource "google_bigquery_dataset" "dv360_test" {
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

  dataset_id                 = "dv360_test"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "amp-main"
}
# terraform import google_bigquery_dataset.dv360_test projects/amp-main/datasets/dv360_test
