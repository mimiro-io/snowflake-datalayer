# Snowflake Datalayer for Mimiro DataHub

[![CI](https://github.com/mimiro-io/datahub-snowflake-layer/actions/workflows/ci.yaml/badge.svg)](https://github.com/mimiro-io/datahub-snowflake-layer/actions/workflows/ci.yaml)

## Usage

Running as from source:
```shell
go run ./cmd/flake
```

Note that the server in default mode expects to be configured for jwt auth.
Provide the following env vars to configure it:
```shell
WELL_KNOWN=https://<authservice>/jwks/.well-known/jwks.json
ISSUER=https://<issuer>
AUDIENCE=https://<audience>
```

You can run it without auth by setting the `AUTHENTICATOR=noop` env var.

The app will run on port 8080 by default. You can change this by setting the `PORT` env var.

## Connecting to Snowflake

To connect to snowflake, prepare a snowflake user for the layer. Then, follow the instructions in the [snowflake docs](https://docs.snowflake.com/en/user-guide/key-pair-auth.html) to generate a keypair for the user.
The following is the process summarized:

### Generating compatible keypairs

Generate your private key:

```shell
openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:2048 -pkeyopt rsa_keygen_pubexp:65537 | openssl pkcs8 -topk8 -nocrypt -outform der > rsa-2048-private-key.p8
```

Generate the public key:
```shell
openssl pkey -pubout -inform der -outform der -in rsa-2048-private-key.p8 -out rsa-2048-public-key.spki
```

Generate base64 url encoded strings from the key files:

```shell
openssl base64 -in rsa-2048-private-key.p8 -out rsa-2048-private-key.base64.p8
openssl base64 -in rsa-2048-public-key.spki -out rsa-2048-public-key.base64.spki
```

You then need to update your user in Snowflake with the public key (not base64).
```
ALTER USER <DB username> SET RSA_PUBLIC_KEY='<paste pub key here>'
```

When running the server, you need to provide the private key as a base64 encoded string.
It can be set in the env var `SNOWFLAKE_PRIVATE_KEY`.

## Convention based usage with minimal configuration

As long as the layer is configured with a valid snowflake connection,
it will be able to read and write to snowflake tables that follow a convention.

### Writing to Snowflake

Writing to snowflake can be done almost configuration free. The layer will create a table for each
dataset that is POSTed to, and write the entities to it.
The layer supports the UDA full sync protocol in addition to incremental writing.
At the end of a fullsync process, it does a table swap, so that there is no downtime for the table.
In Incremental sync, it will append to the table with the new entities.

There is no guarantee for uniqueness of the entities in the table, but the written table rows contain a
recorded timestamp, which can be used to pick the latest duplicates. Also note that entities may be in
a deleted state. use the `deleted` field to filter them out.

### Reading from Snowflake

If a target table contains valid UDA entities in json format, the layer can read from the table without any configuration.
Prerequisites:
- The table must contain a column named `ENTITY` which contains the entity.
- Entities are fully expanded, i.e. no namespace prefixes are used.
- Chronology is reflected by the table's natural order. The latest entity is the last row in the table.

To use convention based reading, construct a dataset name in this form:
```
<database>.<schema>.<table>
```

Then use the dataset name in an entities GET request towards the layer.

```shell
curl http://<layerhost>/datasets/<database>.<schema>.<table>/entities
```

## Usage with Configured Datasets

### Dataset Configuration

The layer uses the `dataset_definitions` part of the common-datalayer configuration to configure the datasets.
For details on the configuration options, see the [documentation](https://github.com/mimiro-io/common-datalayer#data-layer-configuration).

This configuration format can be read from a file or from an url. The layer will look
for the env var `CONFIG_LOCATION`. It will also re-read the configuration file every minute
to allow for dynamic configuration of datasets. The interval can be configured by setting whole
second values in the env var `CONFIG_LOADER_INTERVAL`.

In case a JWT secured URL is used, these configuration options to the layer can be used:
```shell
CONFIG_LOADER_CLIENT_ID
CONFIG_LOADER_CLIENT_SECRET
CONFIG_LOADER_AUDIENCE
CONFIG_LOADER_GRANT_TYPE
CONFIG_LOADER_AUTH_ENDPOINT
```

### Writing to Snowflake

To configure a dataset for writing, add a dataset definition to the configuration with the following fields:
Note that `source_config` is optional. If not provided here, the layer uses the dataset name as table,
and database and schema from the environment variables `SNOWFLAKE_DB` and `SNOWFLAKE_SCHEMA`.

```json
{
  "name": "name of the dataset (uri path)",
  "source_config": {
    "table_name": "name of the table in snowflake",
    "schema": "name of the schema in snowflake",
    "database": "name of the database in snowflake"
  },
  "incoming_mapping_config": {
    "base_uri": "http://example.com",
    "property_mappings": [{
        "Custom": {
            "expression": "expression to extract the value from the entity"
        }, // optional, if set, the layer will use this expression to extract the value from the entity
        "required": true,
        "entity_property": "property name in the entity",
        "property": "name of the column in the table",
        "datatype": "integer", // snowflake datatype, must be compatible with the value
        "is_reference": false, // if true, the value is looked up in the references part of the entity
        "is_identity": false,
        "is_deleted": false,
        "is_recorded": false
    }]
  }
}
```

Every property mapping will be used to create a column in the table. The layer will create the table if it does not exist.
However, the layer will never update existing tables. If you need to change the schema, you need to drop the table first.

A typical property mapping needs to specify the column name: `property`, the column type: `datatype` and the
entity property to take the value from: `entity_property`.

Also note that `entity_property` names must be fully expanded (i.e. no namespace prefixes).

#### Custom expressions for entity properties

Normally, the layer will construct an expression like `$1:props:"name"::string`, given `entity_property=name` and `datatype=string`.
And it will use this expression to extract the column value from each entity.
If a mapping contains a custom expression, it will be applied instead of the default expression.

This can be used to insert static values into the table, or to wrap the json-path based entity access expressions with
additional sql transformation. Possible use cases include unpacking of array values or nested entities.


### Reading from Snowflake

The layer can be configured to read from tables that do not follow the convention based reading.
To do so, create a dataset configuration for layer. The configuration is a json object with the following fields:

```json
{
  "dataset_definitions": [
    {
      "name": "name of the dataset (uri path)",
      "source_config": {
        "table_name": "name of the table in snowflake",
        "schema": "name of the schema in snowflake",
        "database": "name of the database in snowflake",
        "raw_column": "optional name of the column containing a raw json entity"
      },
      "outgoing_mapping_config": { // optional, not used when a raw_column is configured
        "base_uri": "http://example.com",
        "constructions": [{
          "property": "name",
          "operation": "replace",
          "args": ["arg1", "arg2", "arg3"]
        }],
        "property_mappings": [{
          "required": true,
          "entity_property": "property name in the entity",
          "property": "name of the column in the table",
          "datatype": "int", // conversion hint for the layer
          "is_reference": false, // if true, the value is treated as a reference to another entity
          "uri_value_pattern": "http://example.com/{value}",// optional, if set, the value used as string template to construct a property value
          "is_identity": false,
          "default_value": "default"
        }],
        "map_all": true
      }
    }
  ]
}
```

