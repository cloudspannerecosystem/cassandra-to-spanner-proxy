# Enable Compute API
resource "google_project_service" "project_compute" {
  project = var.project_id
  service = "compute.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}


resource "google_compute_instance" "compute_instance" {
  name                      = "${var.name}"
  machine_type              = var.machine_type
  zone                      = var.zone
  allow_stopping_for_update = true
  deletion_protection       = false
  metadata_startup_script   = var.startup_script != "" ? var.startup_script : null

  boot_disk {
    auto_delete = var.disk.auto_delete
    mode        = var.disk.mode

    initialize_params {
      image = var.disk.source_image
      type  = var.disk.disk_type
      size  = var.disk.disk_size_gb
    }
  }

  network_interface {
    network    = var.network
    subnetwork = var.subnetwork
    dynamic "access_config" {
      for_each = var.access_config
      content {
        nat_ip       = access_config.value.nat_ip
        network_tier = access_config.value.network_tier
      }
    }
  }

  labels = var.labels
  service_account {
    # Google recommends custom service accounts that have cloud-platform scope and permissions granted via IAM Roles.
    #    email  = var.email
    scopes = ["cloud-platform"]
  }
}

