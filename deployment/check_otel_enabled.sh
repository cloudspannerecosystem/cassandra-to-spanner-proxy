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

#### This script checks if OTEL is enabled or not and deploys adapter.

# Path to the YAML configuration file
CONFIG_FILE="./sidecar-k8/config.yaml"

# Function to check if OpenTelemetry is enabled
check_otel_enabled() {
    # Using awk to parse the YAML-like structure
    awk '
    # Set flag when otel block starts
    /otel:/ { in_otel_block = 1 }
    
    # Unset flag when otel block ends
    /^$/ { in_otel_block = 0 }

    # When in otel block, look for enabled key
    in_otel_block && /enabled:/ {
        # Check if "True" or "False" specifically
        if ($0 ~ /True/) {
            print "true";
            exit;
        } else if ($0 ~ /False/) {
            print "false";
            exit;
        }
    }' "$CONFIG_FILE"
}

# Call the function and capture the result
result=$(check_otel_enabled)

# Output based on the result
if [ "$result" = "true" ]; then
    echo "OpenTelemetry is enabled."
    echo "-----------------------------"
    kubectl apply -f sidecar-k8/sidecar-deployment.yaml
    cd otel-config
    kubectl -n deployment delete configmap otel-config --ignore-not-found
    kubectl -n deployment create configmap otel-config --from-file config.yaml
else
    echo "OpenTelemetry is not enabled."
    echo "-----------------------------"
    cd k8
    kubectl apply -f proxy-adapter-without-otel.yaml
fi