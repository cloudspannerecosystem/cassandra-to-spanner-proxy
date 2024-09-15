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

resource "google_compute_health_check" "default" {
  name               = "${var.prefix}-tcp-hc-${var.environment}"
  timeout_sec        = 1
  check_interval_sec = 1
  tcp_health_check {
    port_specification = "USE_SERVING_PORT"
  }
}

# backend service
resource "google_compute_backend_service" "default" {
  name                  = "${var.prefix}-lb-${var.environment}"
  protocol              = var.app_protocol
  port_name             = lower(var.app_protocol)
  load_balancing_scheme = "EXTERNAL"
  timeout_sec           = 60
  session_affinity      = "CLIENT_IP"
  health_checks         = [google_compute_health_check.default.id]
  dynamic "backend" {
    for_each = var.neg
    content {
      balancing_mode               = "CONNECTION"
      group                        = "https://www.googleapis.com/compute/v1/projects/${var.project_id}/zones/${backend.value.zone}/networkEndpointGroups/${backend.value.name}"
      max_connections_per_endpoint = var.max_conn_per_endpoint
    }
  }
}

# reserved IP address
resource "google_compute_address" "ip_address" {
  name         = "${var.prefix}-ip-${var.environment}"
  network_tier = var.network_tier
}

resource "google_compute_ssl_certificate" "default" {
  name_prefix = "${var.prefix}-cert-${var.environment}"
  private_key = file(var.private_key_path)
  certificate = file(var.certificate_path)
}

resource "google_compute_ssl_policy" "ssl_policy" {
  name            = "${var.prefix}-ssl-policy-${var.environment}"
  profile         = var.tls_profile
  min_tls_version = var.tls_version
}

resource "google_compute_target_ssl_proxy" "default" {
  name             = "${var.prefix}-target-${var.environment}"
  backend_service  = google_compute_backend_service.default.id
  ssl_certificates = [google_compute_ssl_certificate.default.id]
  # proxy_header     = "PROXY_V1"
  ssl_policy = google_compute_ssl_policy.ssl_policy.self_link
}

resource "google_compute_forwarding_rule" "google_compute_forwarding_rule" {
  name                  = "${var.prefix}-lb-forwarding-rule-${var.environment}"
  region                = var.region
  load_balancing_scheme = "EXTERNAL"
  project               = var.project_id
  ip_protocol           = var.app_protocol
  port_range            = var.app_port
  target                = google_compute_target_ssl_proxy.default.id
  ip_address            = google_compute_address.ip_address.id
  network_tier          = var.network_tier
}

# https://cloud.google.com/load-balancing/docs/https
resource "google_compute_firewall" "lb_fw_allow_health_check" {
  name          = "${var.prefix}-lb-fw-allow-hc-${var.environment}"
  network       = var.network
  direction     = "INGRESS"
  target_tags   = var.firewall_target_tags
  source_ranges = ["130.211.0.0/22", "35.191.0.0/16"]
  allow {
    protocol = lower(var.app_protocol)
    ports    = [var.app_port]
  }
}