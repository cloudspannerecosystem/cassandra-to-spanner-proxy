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

variable "name" {
  description = "A unique name for the resource, required by GCE. Changing this forces a new resource to be created"
  type        = string
}
variable "machine_type" {
  description = "The machine type to create."
  type        = string
}
variable "zone" {
  description = "The zone that the machine should be created in. If it is not provided, the provider zone is used"
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
    source_image = "your_default_image",
    auto_delete  = true,
    boot         = true,
    disk_type    = "your_default_disk_type",
    disk_size_gb = 50,
    mode         = "your_default_mode"
  }
}
variable "network" {
  description = "The name or self_link of the network to attach this interface to. Either network or subnetwork must be provided. If network isn't provided it will be inferred from the subnetwork."
  type        = string
}
variable "subnetwork" {
  description = "The name or self_link of the subnetwork to attach this interface to. Either network or subnetwork must be provided. If network isn't provided it will be inferred from the subnetwork. The subnetwork must exist in the same region this instance will be created in. If the network resource is in legacy mode, do not specify this field. If the network is in auto subnet mode, specifying the subnetwork is optional. If the network is in custom subnet mode, specifying the subnetwork is required."
  type        = string
}
variable "deletion_protection" {
  description = "Enable deletion protection for the instance to prevent accidental deletion."
  default = false
}
variable "access_config" {
  description = "Access configurations, i.e. IPs via which the VM instance can be accessed via the Internet."
  type = list(object({
    nat_ip       = string
    network_tier = string
  }))
  default = []
}
variable "startup_script" {
  description = "It is startup script for install package"
  type = string
}
variable "labels" {
  type = map(string)
  default = {
  }
}