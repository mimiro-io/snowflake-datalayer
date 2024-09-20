# Snowflake Datalayer for Mimiro DataHub

[![CI](https://github.com/mimiro-io/snowflake-layer/actions/workflows/ci.yaml/badge.svg)](https://github.com/mimiro-io/snowflake-layer/actions/workflows/ci.yaml)

## Usage

Running as from source:

```shell
go run ./cmd/snowflake-layer
```

The layer can be configured with a [common-datalayer configuration](https://github.com/mimiro-io/common-datalayer?tab=readme-ov-file#data-layer-configuration)
file. Example for the `layer_config` section:

```json
  "layer_config": {
    "service_name": "snowflake",
    "port": "8080",
    "config_refresh_interval": "600s",
    "log_level": "trace",
    "log_format": "json"
  },
```

Additionally, the layer allows the following environment variables to override
system settings:

```shell
MEMORY_HEADROOM=100 # reject new requests when the layer has less that this many MB free memory
SNOWFLAKE_PRIVATE_KEY=base64 encoded private key
SNOWFLAKE_USER=snowflake user
SNOWFLAKE_ACCOUNT=snowflake account
SNOWFLAKE_DB=snowflake database
SNOWFLAKE_SCHEMA=snowflake schema
SNOWFLAKE_WAREHOUSE=snowflake warehouse
```

## Connecting to Snowflake

To connect to snowflake, prepare a snowflake user for the layer.
Then, follow the instructions in the [snowflake docs](https://docs.snowflake.com/en/user-guide/key-pair-auth.html)
to generate a key pair for the user.

When you have generated an *unencrypted* private key, you need to strip the header and footer lines and remove all whitespaces from the key.
Then it can provided to the service by setting the `SNOWFLAKE_PRIVATE_KEY` environment variable.

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

-   The table must contain a column named `ENTITY` which contains the entity.
-   Entities are fully expanded, i.e. no namespace prefixes are used.
-   Chronology is reflected by the table's natural order. The latest entity is the last row in the table.

To use convention based reading, construct a dataset name in this form:

```sql
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

### Writing to Snowflake

To configure a dataset for writing, add a dataset definition to the configuration with the following fields:
Note that `source_config` is optional. If not provided here, the layer uses the dataset name as table,
and database and schema from the environment variables `SNOWFLAKE_DB` and `SNOWFLAKE_SCHEMA`.

```javascript
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

```javascript
{
    "dataset_definitions": [{
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
            }], "property_mappings": [{
                    "required": true,
                    "entity_property": "property name in the entity",
                    "property": "name of the column in the table",
                    "datatype": "int", // conversion hint for the layer
                    "is_reference": false, // if true, the value is treated as a reference to another entity
                    "uri_value_pattern": "http://example.com/{value}", // optional, if set, the value used as string template to construct a property value
                    "is_identity": false,
                    "default_value": "default"
            }],
            "map_all": true
        }
    }]
}
```
