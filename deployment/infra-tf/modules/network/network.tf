/*
 * Copyright (C) 2024 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

locals {
  combined_ip_cidr_ranges = [var.pub_subnet_cidr_ranges, var.pvt_subnet_cidr_ranges]
}

# VPC Create
resource "google_compute_network" "vpc" {
  name                            = "${var.prefix}-vpc-${var.environment}"
  delete_default_routes_on_create = false
  auto_create_subnetworks         = false
  routing_mode                    = "REGIONAL"
}

# Public Subnet
resource "google_compute_subnetwork" "public_subnet" {
  name                     = "${var.prefix}-pub-subnetwork-${var.environment}"
  ip_cidr_range            = var.pub_subnet_cidr_ranges
  region                   = var.region
  network                  = google_compute_network.vpc.name
  private_ip_google_access = true
}

# Private Subnet
resource "google_compute_subnetwork" "private_subnet" {
  name                     = "${var.prefix}-pvt-subnetwork-${var.environment}"
  ip_cidr_range            = var.pvt_subnet_cidr_ranges
  region                   = var.region
  network                  = google_compute_network.vpc.name
  private_ip_google_access = true
}

# Google Cloud NAT
resource "google_compute_router_nat" "private_router_nat" {
  name                               = "${var.prefix}-pvt-router-nat-${var.environment}"
  router                             = google_compute_router.private_router.name
  region                             = var.region
  nat_ip_allocate_option             = "AUTO_ONLY"
  source_subnetwork_ip_ranges_to_nat = "LIST_OF_SUBNETWORKS"

  subnetwork {
    name                    = google_compute_subnetwork.private_subnet.name
    source_ip_ranges_to_nat = ["ALL_IP_RANGES"]
  }
  depends_on = [google_compute_subnetwork.private_subnet]
}

# Google Cloud NAT ROUTER
resource "google_compute_router" "private_router" {
  name    = "${var.prefix}-pvt-router-${var.environment}"
  region  = var.region
  network = google_compute_network.vpc.name
}

resource "google_compute_firewall" "icmp" {
  name    = "${var.prefix}-allow-icmp-${var.environment}"
  network = google_compute_network.vpc.name

  allow {
    protocol = "icmp"
  }
  priority      = 65534
  source_ranges = ["0.0.0.0/0"]
}

resource "google_compute_firewall" "internal" {
  name    = "${var.prefix}-allow-internal-${var.environment}"
  network = google_compute_network.vpc.name

  allow {
    protocol = "tcp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "udp"
    ports    = ["0-65535"]
  }

  allow {
    protocol = "icmp"
  }

  direction = "INGRESS"
  priority  = 65534

  source_ranges = local.combined_ip_cidr_ranges
}

# https://cloud.google.com/iap/docs/using-tcp-forwarding
resource "google_compute_firewall" "allow_ssh_ingress_from_iap" {
  name          = "${var.prefix}-allow-ssh-ingress-from-iap-${var.environment}"
  network       = google_compute_network.vpc.name
  direction     = "INGRESS"
  source_ranges = ["35.235.240.0/20"]
  allow {
    protocol = "tcp"
    ports    = ["22"]
  }
}
