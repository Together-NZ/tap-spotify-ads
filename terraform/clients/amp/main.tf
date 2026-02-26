provider "google" {
  project = "amp-main"
  region  = "australia-southeast1"
  zone    = "australia-southeast1-a"  # Sydney zone
}

module "bigquerydataset" {
  source = "./BigQueryDataset/australia-southeast1"
}

module "google_artifact_registry_repository" {
    source = "./ArtifactRegistryRepository/australia-southeast1"
}