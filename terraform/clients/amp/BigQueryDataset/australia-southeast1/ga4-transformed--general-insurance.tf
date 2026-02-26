resource "google_bigquery_dataset" "ga4_transformed__general_insurance" {
  access {
    group_by_email = "tahi-service-accounts@wearetogether.co.nz"
    role           = "READER"
  }

  access {
    group_by_email = "tahi-service-accounts@wearetogether.co.nz"
    role           = "roles/bigquery.user"
  }

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

  dataset_id                 = "ga4_transformed__general_insurance"
  delete_contents_on_destroy = false

  labels = {
    managed-by-cnrm = "true"
  }

  location              = "australia-southeast1"
  max_time_travel_hours = "168"
  project               = "amp-main"
}
# terraform import google_bigquery_dataset.ga4_transformed__general_insurance projects/amp-main/datasets/ga4_transformed__general_insurance
