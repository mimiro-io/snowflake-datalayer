FROM golang:1.24-alpine AS build

# Set the Current Working Directory inside the container
WORKDIR /app

# Copy go mod and sum files
COPY go.mod go.sum ./

# Download all dependencies. Dependencies will be cached if the go.mod and go.sum files are not changed
RUN go mod download

# Copy the source from the current directory to the Working Directory inside the container
COPY cmd cmd
COPY internal internal
COPY testdata testdata

# Build the app binaries
RUN go vet ./... && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o snowflake-datalayer cmd/snowflake-datalayer/main.go && \
    go test -v ./...

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /app/snowflake-datalayer /

# server configs
ENV LOG_TYPE=json \
    LOG_LEVEL=info \
    SERVICE_NAME=snowflake-datalayer \
    PORT=8080

# Expose port 8080 to the outside world
EXPOSE 8080

# default command to run the app. override command with snowflake-datalayer to use v2
CMD ["/snowflake-datalayer"]
