/**
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

# Enable Container API
resource "google_project_service" "project_container" {
  project = var.project_id
  service = "container.googleapis.com"

  disable_dependent_services = false
  disable_on_destroy         = false
}

locals {
  master_authorized_networks_config = [{
    cidr_blocks : var.master_authorized_networks
  }]
}

/******************************************
  Create Container Cluster
 *****************************************/
resource "google_container_cluster" "cluster" {
  project = var.project_id

  enable_autopilot = true

  network    = var.network
  subnetwork = var.subnetwork

  name        = var.name
  description = var.description

  location = var.region

  deletion_protection = var.deletion_protection

  release_channel {
    channel = var.release_channel
  }

  dynamic "master_authorized_networks_config" {
    for_each = local.master_authorized_networks_config
    content {
      dynamic "cidr_blocks" {
        for_each = master_authorized_networks_config.value.cidr_blocks
        content {
          cidr_block   = lookup(cidr_blocks.value, "cidr_block", "")
          display_name = lookup(cidr_blocks.value, "display_name", "")
        }
      }
    }
  }

  private_cluster_config {
    enable_private_endpoint = var.enable_private_endpoint
    enable_private_nodes    = var.enable_private_nodes
    master_global_access_config {
      enabled = false
    }
  }

  dynamic "node_pool_auto_config" {
    for_each = length(var.network_tags) > 0 ? [1] : []
    content {
      network_tags {
        tags = var.network_tags
      }
    }
  }

  timeouts {
    create = "45m"
    update = "45m"
    delete = "45m"
  }

}