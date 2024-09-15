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

variable "project_id" {
  description = "The name of the GCP Project where all resources will be launched."
  type        = string
}

variable "repository_name" {
  description = "Name of the Artifact Registry repository"
  type        = string
}

variable "description" {
  description = "Description of the Artifact Registry repository IAM member"
  type        = string
}

variable "region" {
  description = "Google Cloud region where the Artifact Registry repository is located"
  type        = string
}

variable "format" {
  description = "Format of the Artifact Registry repository"
  type        = string
}
