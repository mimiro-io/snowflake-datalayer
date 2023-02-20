build:
	go build -race -o bin/flake cmd/flake/main.go

run:
	bin/flake server \
		--port 9090 \
		--log-level debug \
		--authenticator jwt \
		--well-known="http://localhost:8080/jwks/.well-known/jwks.json" \
		--issuer="https://api.dev.mimiro.io" \
		--audience="https://api.dev.mimiro.io" \
		--snowflake-account=AUONOEH.LU43266 \
		--snowflake-db=DATAHUB_MIMIRO \
		--snowflake-schema=DATAHUB_TEST \
		--snowflake-warehouse=DATAHUB_IMPORT

docker:
	docker build . -t datahub-snowflake-datalayer

test:
	go vet ./...
	go test ./... -v
