resource "google_artifact_registry_repository" "adobe_data_transfer" {
  cleanup_policy_dry_run = true
  format                 = "DOCKER"

  labels = {
    managed-by-cnrm = "true"
  }

  location      = "australia-southeast1"
  mode          = "STANDARD_REPOSITORY"
  project       = "amp-main"
  repository_id = "adobe-data-transfer"
}
# terraform import google_artifact_registry_repository.adobe_data_transfer projects/amp-main/locations/australia-southeast1/repositories/adobe-data-transfer
