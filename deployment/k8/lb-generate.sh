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

# This script reads ports from config file and populates ports dynamically in k8s load balancer service.

# Config file path
CONFIG_FILE="config.yaml"

# Base templates
SERVICE_TEMPLATE="loadbalancer-service.yaml"

# Extract ports
PORTS=$(grep 'port:' $CONFIG_FILE | awk '{print $2}')

# Prepare the ports entries for the Deployment
PORT_ENTRIES=""
for port in $PORTS; do
    PORT_ENTRIES="${PORT_ENTRIES}  - port: $port\n    targetPort: $port\n    protocol: TCP\n    name: port-$port\n"
done

# Replace the placeholder in the Deployment template with actual ports
cp $SERVICE_TEMPLATE "loadbalancer-service-dynamic.yaml"
sed -i "s|# Ports will be populated here dynamically|$PORT_ENTRIES|" "loadbalancer-service-dynamic.yaml"

echo "Load balancer manifest file has been generated."