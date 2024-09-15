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
# Global variable
#---------------------------
variable "project_id" {
  description = "The name of the GCP Project where all resources will be launched."
  type        = string
}
variable "region" {
  description = "Region in which the resource is need to deploy"
  type        = string
}
variable "environment" {
  description = "Environment name"
  type        = string
}
variable "prefix" {
  type        = string
  description = "The prefix to be added to resource names."
}


#---------------------------
# VPC Creation
#---------------------------
variable "pub_subnet_cidr_ranges" {
  type        = string
  description = "A IP CIDR ranges for public subnets."
}
variable "pvt_subnet_cidr_ranges" {
  type        = string
  description = "A IP CIDR ranges for private subnets."
}
