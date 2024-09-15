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

#---------------------------
# Global variables
#---------------------------
variable "project_id" {
  description = "Name of the Google Cloud Platform (GCP) Project where all resources will be launched."
  type        = string
}

variable "region" {
  description = "Region in which the resource needs to be deployed."
  type        = string
}

variable "environment" {
  description = "Name of the environment."
  type        = string
}

variable "prefix" {
  type        = string
  description = "Prefix to be added to resource names."
}

variable "tags" {
  description = "List of target tags for the health check firewall rule and to be applied on GKE nodes."
  type        = list(string)
  default     = []
}

#---------------------------
# VPC Creation
#---------------------------
variable "pub_subnet_cidr_ranges" {
  type        = string
  description = "IP CIDR ranges for public subnets."
}

variable "pvt_subnet_cidr_ranges" {
  type        = string
  description = "IP CIDR ranges for private subnets."
}

#---------------------------
# Load Balancer
#---------------------------
variable "private_key_path" {
  description = "Path to the private key file used for the SSL certificate configuration in the Load Balancer."
  default     = ""
}

variable "certificate_path" {
  description = "Path to the certificate file for the SSL certificate configuration in the Load Balancer."
  default     = ""
}

variable "app_protocol" {
  type        = string
  description = "Protocol used by the application (e.g., HTTP, HTTPS, TCP, etc.)."
}

variable "app_port" {
  type        = number
  description = "Port number on which the application/pod listens."
}

#---------------------------
# Compute Instance
#---------------------------
variable "machine_type" {
  description = "Machine type to be created."
  type        = string
}

variable "zone" {
  description = "Zone in which the machine should be created. If not provided, the provider zone is used."
  type        = string
}

variable "disk" {
  description = "Disk configuration."
  type = object({
    source_image = string
    auto_delete  = bool
    boot         = bool
    disk_type    = string
    disk_size_gb = number
    mode         = string
  })
  default = {
    source_image = "your_default_image"
    auto_delete  = true
    boot         = true
    disk_type    = "your_default_disk_type"
    disk_size_gb = 50
    mode         = "your_default_mode"
  }
}

#---------------------------
# Service Account
#---------------------------

variable "sa_roles" {
  description = "Additional roles to be added to the service account."
  type        = list(string)
  default     = []
}
