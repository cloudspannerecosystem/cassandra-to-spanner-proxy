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
 
FROM golang:1.21 as builder

# Disable cgo to remove gcc dependency
ENV CGO_ENABLED=0

WORKDIR /go/src/cassandra-to-spanner-proxy

# Grab the dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy in source
COPY . ./

# Build and install binary
RUN go install github.com/cloudspannerecosystem/cassandra-to-spanner-proxy

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /go/bin/cassandra-to-spanner-proxy .

# a new clean image with just the binary
FROM alpine:3.19
RUN apk add --no-cache ca-certificates

# Update package list and upgrade vulnerable packages
RUN apk update && \
    apk upgrade && \
    apk add --no-cache busybox openssl

RUN addgroup -S nonroot \
    && adduser -S nonroot -G nonroot

EXPOSE 9042 9043 9044 9045 9046 9047 9048 9049 9050 9051

# Copy in the binary
COPY --from=builder /go/bin/cassandra-to-spanner-proxy  .

COPY config.yaml /

USER nonroot
ENTRYPOINT ["/cassandra-to-spanner-proxy"]
