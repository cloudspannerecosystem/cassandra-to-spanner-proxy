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

# Determine the script's directory
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

# Assuming the script is in the 'systemd' subfolder, calculate the Go project root directory
GO_PROJECT_ROOT="$(cd "$SCRIPT_DIR" && cd .. && pwd)"

# Define the path to the configuration file
CONFIG_FILE="systemd.cfg"

echo "Project Root Directory - $GO_PROJECT_ROOT"

# Ensure the script is executed with root privileges
if [[ "$(id -u)" -ne 0 ]]; then
    echo "This script must be run as root." >&2
    exit 1
fi

# Check if the config file exists
if [ ! -f "$CONFIG_FILE" ]; then
    echo "Config file not found: $CONFIG_FILE"
    exit 1
fi

# Source the config file to export the variables
source $CONFIG_FILE

required_vars=("SPANNER_CONFIG_TABLE" "SPANNER_DB_NAME" "GCP_PROJECT_ID" "SPANNER_INSTANCE")

# Function to check if a variable is set and non-empty
check_var() {
    var_name=$1
    if [ -z "${!var_name}" ]; then  # Indirect variable expansion to check if the variable is set and non-empty
        echo "Required variable $var_name is not set or is blank in the systemd.cfg file. Please add it."
        exit 1
    fi
}

# Iterate over required variables and check each one
for var_name in "${required_vars[@]}"; do
    check_var $var_name
done

echo "All required variables are set.Setting environment variables."

# To truly export them to the environment for other processes to access
export SPANNER_CONFIG_TABLE
export SPANNER_DB_NAME
export GCP_PROJECT_ID
export SPANNER_INSTANCE

# Define service name
SERVICE_NAME="cassandraproxy.service"

# Check if the service is active and stop it
if systemctl is-active --quiet $SERVICE_NAME; then
    echo "Service is currently active. Stopping it..."
    systemctl stop $SERVICE_NAME
fi


# Build the Go application from the project root directory
echo "Building Go application from $GO_PROJECT_ROOT"

cd "$GO_PROJECT_ROOT"
GOOS=linux GOARCH=amd64 go build -o cassandra-to-spanner-proxy -buildvcs=false

# Verify build success
if [[ $? -ne 0 ]]; then
    echo "Failed to build the Go application. Exiting."
    exit 1
fi

# Copy the executable to /usr/local/bin and set permissions
echo "Copying executable to /usr/local/bin/ and setting permissions..."
cp "$GO_PROJECT_ROOT/cassandra-to-spanner-proxy" /usr/local/bin/
chmod 755 /usr/local/bin/cassandra-to-spanner-proxy

# Update the environment variables in the env_cassandra_proxy file
echo "Updating environment variables..."
cat << EOF > /etc/systemd/system/env_cassandra_proxy
SPANNER_CONFIG_TABLE="${SPANNER_CONFIG_TABLE}"
SPANNER_DB_NAME="${SPANNER_DB_NAME}"
GCP_PROJECT_ID="${GCP_PROJECT_ID}"
SPANNER_INSTANCE="${SPANNER_INSTANCE}"
EOF

# Define the service name
SERVICE_NAME="cassandraproxy.service"

# Get the current username (not root)
CURRENT_USER=$(logname)

# Create or update the systemd service file
echo "Creating/updating systemd service file..."
cat << EOF > /etc/systemd/system/$SERVICE_NAME
[Unit]
Description=Cassandra to Spanner Proxy Service

[Service]
ExecStart=/usr/local/bin/cassandra-to-spanner-proxy
EnvironmentFile=/etc/systemd/system/env_cassandra_proxy
User=$CURRENT_USER

# RestartSec=27s
# Optional: Delay for the application restart after the crash.

Restart=always

[Install]
WantedBy=multi-user.target
EOF

# Reload systemd, enable, and start (or restart) the service
systemctl daemon-reload
systemctl enable $SERVICE_NAME
systemctl start $SERVICE_NAME

echo "Setup completed successfully. The service has been enabled and started."
