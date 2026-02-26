resource "google_bigquery_dataset" "ttd" {
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

  access {
    role          = "WRITER"
    user_by_email = "together-meltano@together-internal.iam.gserviceaccount.com"
  }

  access {
    role          = "roles/bigquery.admin"
    user_by_email = "tahi-vision@together-internal.iam.gserviceaccount.com"
  }

  dataset_id                 = "ttd"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "amp-main"
}
# terraform import google_bigquery_dataset.ttd projects/amp-main/datasets/ttd
