terraform {
  backend "gcs" {
    bucket = "amp-main-tfstate"
    prefix = "clients/amp/bq-datasets"
  }
}