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
 
# Global variable
project_id  = "<gcp_project_id>"
region      = "asia-south1"
prefix      = "spanner-adaptor"
environment = "dev"
tags        = ["spanner-adaptor-tag"]

# Network/Subnets
pvt_subnet_cidr_ranges = "10.1.0.0/16"
pub_subnet_cidr_ranges = "10.2.0.0/16"

# LB
private_key_path = "~/Desktop/ssl_certs_lb/privkey.pem"
certificate_path = "~/Desktop/ssl_certs_lb/cert_chain.pem"
app_port         = 9042
app_protocol     = "TCP"

# Kubectl Proxy
machine_type = "e2-micro"
zone         = "asia-south1-a"
disk = {
  source_image = "ubuntu-os-cloud/ubuntu-2004-lts"
  auto_delete  = true,
  boot         = true,
  disk_type    = "pd-balanced",
  disk_size_gb = 30,
  mode         = "READ_WRITE"
}

# Service Account
sa_roles = ["roles/spanner.databaseAdmin", "roles/spanner.viewer"]

