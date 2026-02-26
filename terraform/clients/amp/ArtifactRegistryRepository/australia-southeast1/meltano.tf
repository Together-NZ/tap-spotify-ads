resource "google_artifact_registry_repository" "meltano" {
  description = "Meltano repository"
  format      = "DOCKER"

  labels = {
    managed-by-cnrm = "true"
  }

  location      = "australia-southeast1"
  mode          = "STANDARD_REPOSITORY"
  project       = "amp-main"
  repository_id = "meltano"
}
# terraform import google_artifact_registry_repository.meltano projects/amp-main/locations/australia-southeast1/repositories/meltano
