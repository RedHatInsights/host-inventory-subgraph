# host-inventory-subgraph

This is an implementation of a custom xjoin subgraph. It is based off the [xjoin-subgraph-template](https://github.com/RedHatInsights/xjoin-subgraph-template) repo.

### Dependencies

- Node.js v16
- NPM 8.1.0
- Typescript 4.6.2
- A running instance of an [Apicurio schema registry](https://www.apicur.io/registry/)
- A running instance of Elasticsearch

### Environment Variables

| Name                     | Description                                                     | Default Value         |
|--------------------------|-----------------------------------------------------------------|-----------------------|
| NODE_CONFIG_DIR          | Directory of config variables                                   | dist/config           |
| PORT                     | Port to use for the xjoin-api-subgraph service                  | 4000                  |
| AVRO_SCHEMA              | The avro schema to use for generating the GraphQL APIs          | {}                    |
| ELASTIC_SEARCH_URL       | The full URL to a running Elasticsearch instance                | http://localhost:9200 |
| ELASTIC_SEARCH_USERNAME  | The username for the connection to Elasticsearch                | xjoin                 |
| ELASTIC_SEARCH_PASSWORD  | The username for the connection to Elasticsearch                | xjoin1337             |
| ELASTIC_SEARCH_INDEX     | The index to query against                                      | xjoin.inventory.hosts |
| SCHEMA_REGISTRY_PROTOCOL | The protocol to use for the Apicurio schema registry connection | http                  |
| SCHEMA_REGISTRY_HOSTNAME | The hostname to use for the Apicurio schema registry connection | localhost             |
| SCHEMA_REGISTRY_PORT     | The port to use for the Apicurio schema registry connection     | 1080                  |
| LOG_LEVEL                | The log level                                                   | debug                 |

### Running the server

Set each environment variable to the value specific to your environment before running the server.

```shell
npm run start
```

### Running the tests

The tests use mocks, so they don't require a running instance of Elasticsearch or Apicurio.

```shell
npm run test
```