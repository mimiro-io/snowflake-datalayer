# Snowflake Datalayer for Mimimro DataHub

## Usage

The flake binary can be used in 2 modes; server and client.


Running as a cli:
```shell
bin/flake run \
  --file=input-uda.json \
  --snowflake-user="" \
  --snowflake-password="" \
  --snowflake-account="" \
  --snowflake-db="" \
  --snowflake-schema="" \ 
  --snowflake-connection-string="" 
```
When running as a cli, you need to provide a file of valid uda json, you can use `mim dataset entities --raw > out.json` 
to get a valid input file. There are sanity checks in the cli to prevent a huge file from blowing up the machine.


Running as a server:
```shell
bin/flake server \
  --port=8080 \
  --log-type=json \
  --snowflake-user="" \
  --snowflake-password="" \
  --snowflake-account="" \
  --snowflake-db="" \
  --snowflake-schema="" \ 
  --snowflake-connection-string="" \
  --well-known="" \
  --issuer="" \
  --audience="" \
  --authenticator="jwt" 
```

When running as a server, you need to deal with security. By providing an url to a well known endpoint, together with an
audience and an issuer, a jwt Bearer token will be validated. Currently only tokens containing a scope "datahub:w" will 
be allowed to write to the endpoint.

You can also enable log-type=json when running a server to get json formatted log output.