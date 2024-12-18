 # Copyright (C) 2024 Google LLC
 #
 # Licensed under the Apache License, Version 2.0 (the "License"); you may not
 # use this file except in compliance with the License. You may obtain a copy of
 # the License at
 #
 #   http://www.apache.org/licenses/LICENSE-2.0
 #
 # Unless required by applicable law or agreed to in writing, software
 # distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 # WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 # License for the specific language governing permissions and limitations under
 # the License.
 
#!/bin/bash

# This file populates ports dynamically in the k8s manifest file(without otel sidecar)

# Config file path
CONFIG_FILE="config.yaml"

# Base templates
DEPLOYMENT_TEMPLATE="proxy-adapter-deployment.yaml"

# Extract ports
PORTS=$(grep 'port:' $CONFIG_FILE | awk '{print $2}')

# Prepare the ports entries for the Deployment
PORT_ENTRIES=""
for port in $PORTS; do
    PORT_ENTRIES="${PORT_ENTRIES}        - containerPort: $port\n          protocol: TCP\n"
done

# Replace the placeholder in the Deployment template with actual ports
cp $DEPLOYMENT_TEMPLATE "proxy-adapter-without-otel.yaml"
sed -i "s|# Ports will be populated here dynamically|$PORT_ENTRIES|" "proxy-adapter-without-otel.yaml"

echo "Deployment without otel sidecar has been configured."