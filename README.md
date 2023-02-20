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

## Connecting to Snowflake

Currently this layer is keyed for Mimiro Snowflake only, so you must use a cert. Not that this code doesn't yet support 
encrypted certificates, you you will have to regenerate the certs once this works.

### Generating compatible certs

Generate your certificate:

```shell
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -pkeyopt rsa_keygen_pubexp:65537 | openssl pkcs8 -topk8 -nocrypt -outform der > rsa-2048-private-key.p8
```

Generate the public key:
```shell
openssl pkey -pubout -inform der -outform der -in rsa-2048-private-key.p8 -out rsa-2048-public-key.spki
```

Generate base64 url encoded strings from the binary certificate files:
```shell
make flake
bin/flake encode --input rsa-2048-private-key.p8
bin/flake endoce --input rsa-2048-public-key.spki
```

You then need to start the flake command with the output from the private cert, and you need to update your user
in Snowflake with the public key.
