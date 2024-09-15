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
 
module "vpc" {
  source                 = "./modules/network"
  project_id             = var.project_id
  pub_subnet_cidr_ranges = var.pub_subnet_cidr_ranges
  pvt_subnet_cidr_ranges = var.pvt_subnet_cidr_ranges
  region                 = var.region
  prefix                 = var.prefix
  environment            = var.environment
}

module "neg" {
  source      = "./modules/network-endpoint-group"
  project_id  = var.project_id
  region      = var.region
  prefix      = var.prefix
  environment = var.environment
  network     = module.vpc.vpn_name
  subnet      = module.vpc.subnet_name_pvt
}

module "lb" {
  source                = "./modules/load-balancer"
  project_id            = var.project_id
  region                = var.region
  environment           = var.environment
  network               = module.vpc.vpn_name
  prefix                = var.prefix
  neg                   = module.neg.network_endpoint_groups
  max_conn_per_endpoint = 100
  private_key_path      = var.private_key_path
  certificate_path      = var.certificate_path
  app_port              = var.app_port
  app_protocol          = var.app_protocol
  tls_version           = "TLS_1_2"
  network_tier          = "STANDARD"
  tls_profile           = "COMPATIBLE"
  firewall_target_tags  = var.tags
}

module "gke" {
  source                          = "./modules/gke-autopilot"
  project_id                      = var.project_id
  name                            = "${var.prefix}-gke-${var.environment}"
  region                          = var.region
  network                         = module.vpc.vpn_name
  subnetwork                      = module.vpc.subnet_name_pvt
  release_channel                 = "REGULAR"
  enable_private_endpoint         = true
  enable_private_nodes            = true
  network_tags                    = var.tags
  deletion_protection             = false
  master_authorized_networks = [
    {
      cidr_block   = var.pvt_subnet_cidr_ranges
      display_name = "Pvt Subnet Cidr"
    },
  ]
}

module "compute_instance" {
  source              = "./modules/compute_instance"
  project_id          = var.project_id
  region              = var.region
  zone                = var.zone
  environment         = var.environment
  network             = module.vpc.vpn_name
  subnetwork          = module.vpc.subnet_name_pvt
  name                = "kubectl-proxy-vm-${var.environment}"
  machine_type        = var.machine_type
  disk                = var.disk
  deletion_protection = false
  startup_script      = <<-SCRIPT
    #!/bin/bash
    # Install Tinyproxy
    sudo apt update
    sudo apt install tinyproxy -y
    sudo sed -i 's/^#\(Allow 127.0.0.1\)/\1/' /etc/tinyproxy/tinyproxy.conf
    sudo service tinyproxy restart
  SCRIPT
}

module "service_account" {
  source      = "./modules/service_account"
  project_id  = var.project_id
  region      = var.region
  name        = "${var.prefix}-proxy-sa-${var.environment}"
  description = "This service account will be used in pods for authentication to Spanner."
  roles       = var.sa_roles
}

module "repositories" {
  source              = "./modules/artifact-registry"
  project_id          = var.project_id
  region              = var.region
  repository_name     = "${var.prefix}-docker-${var.environment}"
  description         = "Artifact Registry containing Docker images for the ${var.prefix} project."
  format              = "DOCKER"
}