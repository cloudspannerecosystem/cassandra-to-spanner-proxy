# Configure Proxy adaptor as sidecar

## Overview

Let's take an example. In this scenario, the demo-app serves as the application, and the proxy-adaptor functions as the sidecar. Our objective is to perform CRUD operations on the demo-app.

> Make a modification in the application configuration by updating the port number for the proxy-adaptor to 9042. Ensure that the demo-app is correctly configured.

## Getting Started

Follow the steps below to set up and deploy the Spanner Adaptor:

### 1. Prepare Google Cloud CLI Config

```bash
export PROJECT_ID="<gcp_project_id>"
export REGION=<region>
gcloud auth application-default login --project $PROJECT_ID
gcloud config set project $PROJECT_ID
```

### 2. Create Service Accounts

```bash
export SERVICE_ACCOUNT=adaptor-proxy-sa

gcloud iam service-accounts create $SERVICE_ACCOUNT \
    --description="Service Account for Spanner Adaptor proxy" \
    --display-name="Service Account for Spanner Adaptor proxy"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/spanner.databaseAdmin"

gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" \
    --role="roles/spanner.viewer"
```

### 3. Create JSON Key for Service Account

```bash
gcloud iam service-accounts keys create --iam-account "$SERVICE_ACCOUNT@$PROJECT_ID.iam.gserviceaccount.com" ~/Downloads/spanner-adaptor-service-account.json
```

### 4. Kubernetes Configuration

#### a. Create Namespace and Secret

```bash
# Create namespace
kubectl create namespace deployment

# Create a secret in Kubernetes named app-secret-sa in the deployment namespace.
kubectl create secret generic secret-sa \
  --from-file ~/Downloads/<spanner-adaptor-service-account-json-file-name> -n deployment
```

#### b. Modify YAML File

Modify the YAML file: like `GOOGLE_APPLICATION_CREDENTIALS`, `SPANNER_DB_NAME` and `SPANNER_NUM_CHANNELS`

```bash
vi application-with-sidecar.yaml
```

#### c. Apply Configuration

```bash
kubectl apply -f spanner-deployment.yaml
```

### 5. Verify Deployment

Login to the application pod:

```bash
kubectl exec -it application-sidecar-<pod-id> -c demo-app -n deployment -- bash
telnet localhost 9042
```
