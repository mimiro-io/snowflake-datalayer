FROM golang:1.21-alpine as build_env

# Install git + SSL ca certificates.
# Git is required for fetching the dependencies.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache \
    git \
    gcc \
    musl-dev \
    ca-certificates \
    tzdata && \
    update-ca-certificates

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

FROM build_env as builder

# Copy the source from the current directory to the Working Directory inside the container
COPY cmd cmd
COPY internal internal
COPY testdata testdata

# Build the app binaries
RUN go vet ./... && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o snowflake-datalayer cmd/snowflake-datalayer/main.go && \
    go test -v ./...

# enable the apps to run as any non root user
RUN chgrp 0 snowflake-datalayer && chmod g+X snowflake-datalayer

FROM scratch

WORKDIR /root/

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/snowflake-datalayer .
COPY --from=builder /etc/passwd /etc/passwd

# server configs
ENV LOG_TYPE=json \
    LOG_LEVEL=info \
    SERVICE_NAME=snowflake-datalayer \
    PORT=8080 \
    HOME=/ \
    USER=5678

# Expose port 8080 to the outside world
EXPOSE 8080

# set a non root user
USER 5678

# default command to run the app. override command with snowflake-datalayer to use v2
CMD ["./snowflake-datalayer"]
