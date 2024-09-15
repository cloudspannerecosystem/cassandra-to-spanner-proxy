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
  description = "Name of the GCP Project where all resources will be launched."
  type        = string
}

variable "region" {
  description = "Region in which the resource needs to be deployed."
  type        = string
}

variable "environment" {
  description = "Environment name."
  type        = string
}

variable "prefix" {
  type        = string
  description = "Prefix to be added to resource names."
}

variable "firewall_target_tags" {
  description = "List of target tags for the health check firewall rule."
  type        = list(string)
  default     = []
}

#---------------------------
# Load Balancer
#---------------------------

variable "neg" {
  description = "List of network-endpoint groups to configure LB backend."
  type        = list(object({
    name = string
    zone = string
  }))
}

variable "max_conn_per_endpoint" {
  type        = number
  description = "Maximum number of connections per endpoint in the network-endpoint group."
}

variable "private_key_path" {
  description = "Path to the private key file."
  default     = ""
}

variable "certificate_path" {
  description = "Path to the certificate file."
  default     = ""
}

variable "network" {
  description = "Name of the VPN to be created."
}

variable "app_protocol" {
  type        = string
  description = "Protocol used by the application (e.g., HTTP, HTTPS, TCP, etc.)."
}

variable "app_port" {
  type        = number
  description = "Port number on which the application listens."
}

variable "tls_version" {
  type        = string
  description = "Minimum TLS version supported by the application (e.g., 'TLSv1.2', 'TLSv1.3')."
}

variable "tls_profile" {
  type        = string
  description = "TLS profile used for the load balancer. Acceptable values include 'COMPATIBLE', 'MODERN', or 'RESTRICTED'. The 'COMPATIBLE' profile allows a broad range of clients, 'MODERN' enhances security by supporting modern clients only, and 'RESTRICTED' enforces the highest security standards with more limited client compatibility."
}

variable "network_tier" {
  type        = string
  description = "Networking tier for the load balancer. Acceptable values include 'PREMIUM' or 'STANDARD'. The 'PREMIUM' tier provides advanced features, while the 'STANDARD' tier is suitable for basic workloads."
}