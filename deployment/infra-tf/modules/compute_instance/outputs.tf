output "id" {
  description = "An identifier for the resource with format projects/{{project}}/zones/{{zone}}/instances/{{name}}"
  value       = google_compute_instance.compute_instance[*].id
}

