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

output "vpc_name" {
  value = module.vpc.vpn_name
}

output "subnet_name_pvt" {
  value = module.vpc.subnet_name_pvt
}

output "subnet_name_pub" {
  value = module.vpc.subnet_name_pub
}

output "lb_ip" {
  value = module.lb.load_balancer_ip
}

output "server_account" {
  value = module.service_account.email
}

output "kubectl_proxy_vm" {
  value = "kubectl-proxy-vm-${var.environment}"
}

output "artifact-registry" {
  value = module.repositories.artifact-registry
}

output "gke_cluster" {
  value = module.gke.gke_name
}
