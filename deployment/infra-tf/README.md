## Terraform Infrastructure for Spanner Adaptor

This Terraform code automates the provisioning of infrastructure for a Spanner Adaptor, including the following components:

1. **VPN Creation:**
   - Sets up a Virtual Private Network (VPN) with private and public subnets.

2. **Network Load Balancer Configuration:**
   - Configures a Network Load Balancer with SSL termination and a backend composed of a Network Endpoint Group.

3. **GKE Cluster Creation:**
   - Creates a Google Kubernetes Engine (GKE) cluster in autopilot mode within the private GKE cluster.

4. **Compute Instance Provisioning:**
   - Deploys a compute instance, utilized for connecting to the private GKE cluster.

5. **Artifact Registry for Spanner Adaptor Docker Image:**
   - Establishes an Artifact Registry to store the Docker image for the Spanner Adaptor.

6. **Service Account Creation:**
   - Sets up a service account designed for pod authentication to Spanner.

## Prerequisites

Ensure you have the following before getting started:

1. **GCP Editor Role Permissions:**
   - Obtain the GCP Editor role permissions to manage resources on Google Cloud Platform.

2. **SSL Certificate:**
   - Acquire an SSL certificate for lb configuration.

3. **Terraform CLI:**
   - Install Terraform CLI on your machine. Download it from [Terraform Downloads](https://www.terraform.io/downloads.html).

4. **kubectl CLI:**
   - Install `kubectl` CLI for Kubernetes cluster deployment. Follow the [Kubernetes Documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/) for installation instructions.

### SSL certificate:

   >Ensure that the validated SSL certificate is stored in your local system at the specified location below.
   ```bash
   private_key_path = "~/Desktop/ssl_certs_lb/privkey.pem"
   certificate_path = "~/Desktop/ssl_certs_lb/cert_chain.pem"
   ```

### Usage Instructions:

1. Clone the repository:

   ```bash
   git clone git@github.com:cldcvr/cassandra-to-spanner-proxy.git
   ```

2. Prepare Google Cloud CLI Config:
   ```bash
   export PROJECT_ID=<project_id>
   export REGION=<region>
   gcloud auth application-default login --project $PROJECT_ID
   gcloud config set project $PROJECT_ID
   ```

3. Create Bucket to Store Terraform state.
   ```bash
   gcloud storage buckets create gs://tf-cassandra-to-spanner-bucket --location=$REGION
   ```

4. Update the values in the `terraform.tfvars` file, such as project_id, region, Subnets, and the respective certificate generator locations, as outlined in the example below.
   ```bash
   cd cassandra-to-spanner-proxy/deployment/infra-tf
   
   vi terraform.tfvars
   # Global variable
   project_id  = "<project_id>"
   region      = "asia-south1"
   prefix      = "spanner-adaptor"
   environment = "dev"
   tags        = ["spanner-adaptor"]
   
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
   ```
   
5. Initialize the Terraform configuration:

   ```bash
    terraform init -backend-config=bucket="tf-cassandra-to-spanner-bucket" -backend-config=prefix="terraform/tfstate/dev" 
   ```

6. Plan & Apply the configuration:

   ```bash
    terraform plan
    terraform apply
   ```
   
7. Terraform Output:
   ```bash
   artifact-registry = "spanner-adaptor-docker-dev"
   gke_cluster = "spanner-adaptor-gke-dev"
   kubectl_proxy_vm = "kubectl-proxy-vm-dev"
   lb_ip = "xxx.xxx.xxx.xxx"
   server_account = "spanner-adaptor-proxy-sa-dev@<project_id>.iam.gserviceaccount.com"
   subnet_name_pub = "spanner-adaptor-pub-subnetwork-dev"
   subnet_name_pvt = "spanner-adaptor-pvt-subnetwork-dev"
   vpc_name = "spanner-adaptor-vpc-dev"
   ```

## Terraform Compatibility

This Terraform configuration requires Terraform version 0.13 or later. The required Google provider and its version are specified as follows:

```hcl
terraform {
  required_version = ">= 0.13"
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.1.0"
    }
  }
}
```

## Enable APIs
In order to operate with the Service Account you must activate the following APIs on the project where the Service Account was created:

* Compute Engine API - compute.googleapis.com
* Kubernetes Engine API - container.googleapis.com
* Artifact Registry API - artifactregistry.googleapis.com