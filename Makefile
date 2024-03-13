define empty_config
{
  "layer_config": {
    "service_name": "snowflake-local",
    "port": "8080"
  },
  "system_config": {
    "memory_headroom": 100,
    "snowflake_db": "db",
    "snowflake_schema": "schema",
    "snowflake_user": "user",
    "snowflake_account": "acct",
    "snowflake_warehouse": "wh",
	"snowflake_private_key": "MIIBUwIBADANBgkqhkiG9w0BAQEFAASCAT0wggE5AgEAAkEAxIXbFdo7AhvdobX4F+gjkgGD3wM2zH6GhvJSnCLmKvlYPGwwX9J+xgEBPLSEH4R4zW/YFySOYxGU/DboZIpXfwIDAQABAkBKOch643cgH8hBGMrAtNQihGH7bGpZKHzFIWdkQ6YtmmBu/O5FtBNJQgsFsWnOydURrJzGoG1ezMQArNBdFUUJAiEA40p9KnnaA/NWb608yolfArKHcQJ+iXx1d2HkeVMbCSUCIQDdWHj+0VWZ00iNh5plqFov8EKNAMImYEi/1geBHcQ20wIgeaNGovG9NDoI+xEqJHYp66ahh2A/WdLKho5UGH3aTSUCIBqeDgbOk5Wo87uZR/bblOTY5pfgNHi68WSoT0S2mKbjAiBnG28oMs8D+vGKZMawf2BKbq33MjRsMJmcjmMHJqy7ow=="
  },
  "dataset_definitions": []
}
endef
export empty_config

build:
	go build -race -o bin/snowflake-datalayer ./cmd/snowflake-datalayer

.SHELL=bash
run:
	mkdir -p /tmp/sfconf && echo $$empty_config > /tmp/sfconf/config.json && \
		DATALAYER_CONFIG_PATH=/tmp/sfconf go run ./cmd/snowflake-datalayer

test:
	go vet ./...
	go test ./... -v

license:
	go install github.com/google/addlicense; addlicense -c "MIMIRO AS" $(shell find . -iname "*.go")
