resource "google_artifact_registry_repository" "cloud_run_source_deploy" {
  description = "Cloud Run Source Deployments"
  format      = "DOCKER"

  labels = {
    managed-by-cnrm = "true"
  }

  location      = "australia-southeast1"
  mode          = "STANDARD_REPOSITORY"
  project       = "amp-main"
  repository_id = "cloud-run-source-deploy"
}
# terraform import google_artifact_registry_repository.cloud_run_source_deploy projects/amp-main/locations/australia-southeast1/repositories/cloud-run-source-deploy
