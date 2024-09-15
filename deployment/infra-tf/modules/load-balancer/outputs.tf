output "load_balancer_ip" {
  value = google_compute_forwarding_rule.google_compute_forwarding_rule.ip_address
}