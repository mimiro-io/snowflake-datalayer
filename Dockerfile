FROM golang:1.20-alpine3.17 as build_env

# Install git + SSL ca certificates.
# Git is required for fetching the dependencies.
# Ca-certificates is required to call HTTPS endpoints.
RUN apk update && apk add --no-cache git gcc musl-dev ca-certificates tzdata && update-ca-certificates

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy the source from the current directory to the Working Directory inside the container
COPY . .

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

FROM build_env as builder

# Build the Go app
RUN go vet ./...
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -ldflags="-w -s" -o flake cmd/flake/main.go

# enable the app to run as any non root user
RUN chgrp 0 flake && chmod g+X flake

FROM scratch

WORKDIR /root/

COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /app/flake .

# server configs
ENV LOG_TYPE=json \
  LOG_LEVEL=info \
  SERVICE_NAME=datahub-snowflake-datalayer \
  PORT=8080 \
  SNOWFLAKE_USER=<user> \
  SNOWFLAKE_PASSWORD=<password> \
  SNOWFLAKE_ACCOUNT=<account> \
  SNOWFLAKE_DB=<db> \
  SNOWFLAKE_SCHEMA=<schema> \
  WELL_KNOWN=https://auth.dev.mimiro.io/jwks/.well-known/jwks.json \
  ISSUER=https://api.dev.mimiro.io \
  AUDIENCE=https://api.dev.mimiro.io \
  AUTHENTICATOR=jwt

# Expose port 8080 to the outside world
EXPOSE 8080

# set a non root user
USER 5678

CMD ["./flake", "server"]
