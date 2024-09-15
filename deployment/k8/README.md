# Spanner Adaptor Deployment on GKE

Deploying the Spanner Adaptor on Google Kubernetes Engine (GKE) facilitates seamless communication between your application and Google Cloud Spanner. Below is an overview of the major steps involved in the deployment process.

## Prerequisites

Ensure you have the following before getting started:

1. **GCP Editor Role Permissions:**
   - Obtain the GCP Editor role permissions to manage resources on Google Cloud Platform.
2. **kubectl CLI:**
   - Install `kubectl` CLI for Kubernetes cluster deployment. Follow the [Kubernetes Documentation](https://kubernetes.io/docs/tasks/tools/install-kubectl/) for installation instructions.
3. **Docker:**
   - Build docker image and push to Artifact Registry.

## Usage Instructions:

### Step 1: Build and Push Docker Image

Clone the repository:

```bash
git clone git@github.com:cldcvr/cassandra-to-spanner-proxy.git
```

Prepare Google Cloud CLI Config:

```bash
export PROJECT_ID=<project_id>
export REGION=<region>
gcloud auth application-default login --project $PROJECT_ID
gcloud config set project $PROJECT_ID
```

Retrieve values from the Terraform output and export them as environment variables:

```bash
terraform output
```

```bash
export SERVICE_ACCOUNT="spanner-adaptor-proxy-sa-dev@<project_id>.iam.gserviceaccount.com"
export ARTIFACT_REGISTRY="spanner-adaptor-docker-dev"
export APP_NAME="spanner-adaptor"
export PROXY_VM_NAME="kubectl-proxy-vm-dev"
export GKE_CLUSTER="spanner-adaptor-gke-dev"
```

Configure Google Cloud Artifact Registry:

```bash
gcloud auth configure-docker $REGION-docker.pkg.dev
```

Build and Push Docker Image:

```bash
cd cassandra-to-spanner-proxy
docker build --platform linux/amd64 -t $REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REGISTRY/"$APP_NAME":tag1 .
docker push $REGION-docker.pkg.dev/$PROJECT_ID/$ARTIFACT_REGISTRY/"$APP_NAME":tag1
```

### Step 2: Create Secrets and Deploy Spanner Adaptor

Create a JSON Key for Google Cloud Service Account:

```bash
gcloud iam service-accounts keys create --iam-account "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com"~/Downloads/spanner-adaptor-service-account.json
```

Modify the YAML file: like `GOOGLE_APPLICATION_CREDENTIALS`, `SPANNER_DB_NAME` and `SPANNER_NUM_CHANNELS`

```bash
vi spanner-deployment.yaml
```

Make the necessary updates in the file and save the changes.

### Step 3: Establish a Connection to the GKE Private Cluster

Use the `kubectl-proxy-vm-dev` to connect to the Private GKE Cluster:

```bash
gcloud container clusters get-credentials $GKE_CLUSTER --region $REGION --project $PROJECT_ID
gcloud compute ssh $PROXY_VM_NAME --tunnel-through-iap --project=$PROJECT_ID --zone=${REGION}-a --ssh-flag="-4 -L8888:localhost:8888 -N -q -f"
export HTTPS_PROXY=localhost:8888
kubectl get pod -A
```

Unset IAP Tunneling:

```bash
gcloud config unset proxy/type
gcloud config unset proxy/address
gcloud config unset proxy/port
unset HTTPS_PROXY
unset HTTP_PROXY
```

Create Secrets:

```bash
kubectl create namespace deployment
kubectl create secret generic app-secret-sa -n deployment \
  --from-file ~/Downloads/<spanner-adaptor-service-account-json-file-name>
```

### Step 4: Deploy Spanner Adaptor

Once GKE cluster access is obtained, proceed with the deployment:

```bash
cd k8/
kubectl apply -f spanner-deployment.yaml
```
