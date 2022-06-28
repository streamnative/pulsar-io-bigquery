---
dockerfile: "https://hub.docker.com/r/streamnative/pulsar-io-bigquery"
alias: Google Cloud BigQuery Sink Connector
---

The [Google Cloud BigQuery](https://cloud.google.com/bigquery) sink connector pulls data from Pulsar topics and persists data to Google Cloud BigQuery tables.

![](/docs/google-bigquery-sink.png)

# Features
This section describes Google BigQuery Sink connector features.

## At least once delivery

Pulsar connector framework provides three guarantees: `at-most-once`, `at-least-once` and `effectively-once`.

The connector can provide the `at-least-once` delivery at most, and when the user configures the `effectively-once`, it will throw exception.

## Auto create and update tables schema

The connector support auto create and update tables schema by pulsar topic schema. Can be controlled by the following configuration:
```
autoCreataTables = true
autoUpdateSchema = true
```
The connector supports mapping structures schema, structures are mapped to BigQuery [RECORD TYPE](https://cloud.google.com/bigquery/docs/nested-repeated#example_schema).

Also support write some pulsar system field. Refer to the configuration below:
```
#
# optional: __schema_version__ , __partition__   , __event_time__    , __publish_time__  
#           __message_id__     , __sequence_id__ , __producer_name__ , __key__           , __properties__ 
#
defaultSystemField = __event_time__,__message_id__
```

The update schema currently adopts the merge rule, when it is found that the pulsar topic schema and bigquery schema are different, bigquery will merge the two schemas.

> That bigquery will not delete fields. If you change the name of a field in pulsar topic, the connector preserves both fields.


The schemas that currently support conversion are:

| schema          | support |
|-----------------|---------|
| AVRO            | Yes     |
| PRIMITIVE       | Yes     |
| JSON            | Doing   |
| KEY_VALUE       | Plan    |
| PROTOBUF        | Plan    |
| PROTOBUF_NATIVE | Plan    |

## Partitioned tables and clustered tables
> This feature takes effect only when autoCreateTable = true. If you create a table manually, you need to manually spcify the partition key and cluster table key.

### Partitioned tables
BigQuery supports partitioned tables. Partitioned tables can improve query and control costs by reducing the data read from the table by the query.
This connector will provide a switch for users to choose whether to create a partition table, it will use __event_time__ the partition key.
```
partitioned-tables = true
```

### Clustered tables
Clustered tables can improve query a partition. This connector will provide a switch for users to choose whether to create a clustered table, it will use __message_id__ the partition key.
```
clustered-tables = true
```

## Multiple tasks
The parallelism of Sink execution can be configured, use the scheduling mechanism of the Function itself, and multiple sink instances will be scheduled to run on different worker nodes. Multiple sinks will consume message together according to the configured subscription mode.

```
parallelism = 4
```

> When you encounter write bottlenecks, increasing parallelism is an effective way. Also, you need to pay attention to whether the write rate is greater than [BigQuery Rate Limits](https://cloud.google.com/bigquery/quotas#streaming_inserts)

## Batch progress

In order to increase write throughput, this connector does batch internally. You can control to write batch size and latency through the following parameters.

```
batchMaxSize = 100
batchMaxTime = 4000
batchFlushIntervalTime = 2000
```

# How to get
This section describes how to build the Google BigQuery sink connector.

## Work with Function Worker

You can get the Google BigQuery sink connector using one of the following methods if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

- Download the JAR package from [the download page](https://github.com/streamnative/pulsar-io-bigquery/releases/download/v{{connector:version}}/pulsar-io-bigquery-{{connector:version}}.jar).

- Build it from the source code.

To build the Google BigQuery sink connector from the source code, follow these steps.

1. Clone the source code to your machine.

   ```bash
   git clone https://github.com/streamnative/pulsar-io-bigquery
   ```

2. Build the connector in the `pulsar-io-bigquery` directory.

   ```bash
   mvn clean install -DskipTests
   ```

   After the connector is successfully built, a `JAR` package is generated under the target directory.

   ```bash
   ls target
   pulsar-io-bigquery-{{connector:version}}.jar
   ```

## Work with Function Mesh
You can pull the Google BigQuery sink connector Docker image from the [Docker Hub](https://hub.docker.com/r/streamnative/pulsar-io-bigquery) if you use [Function Mesh](https://functionmesh.io/docs/connectors/run-connector) to run the connector.

# How to configure 

Before using the Google BigQuery sink connector, you need to configure it. This table lists the properties and the descriptions.

| Name                          | Type    | Required | Default           | Description                                                                                                                                                                                                                                                                      |
|-------------------------------|---------|----------|-------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `projectId`                   | String  | true     | "" (empty string) | The Google BigQuery project id.                                                                                                                                                                                                                                                  |
| `datasetName`                 | String  | true     | "" (empty string) | The Google BigQuery dataset name.                                                                                                                                                                                                                                                |
| `tableName`                   | String  | true     | "" (empty string) | The Google BigQuery table name.                                                                                                                                                                                                                                                  |
| `credentialJsonString`        | String  | true     | "" (empty string) | Authentication key, use the environment variable to get the key when key is empty. Key acquisition reference [Google Doc](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries#before-you-begin).                                                      |
| `visibleModel`                | String  | false    | "Committed"       | Optional `Committed` or `Pending`. The mode controls when data written to the stream becomes visible in BigQuery for reading. Refer [Google Doc](https://cloud.google.com/bigquery/docs/write-api#application-created_streams).                                                  |
| `pendingMaxSize`              | int     | false    | 10000             | Maximum number of messages waiting to be committed in `Pending` visibility mode.                                                                                                                                                                                                 |
| `batchMaxSize`                | int     | false    | 20                | Maximum number of batch messages.                                                                                                                                                                                                                                                |
| `batchMaxTime`                | long    | false    | 5000              | Batch max wait time: milliseconds.                                                                                                                                                                                                                                               |
| `batchFlushIntervalTime`      | long    | false    | 2000              | Batch trigger flush interval time: milliseconds.                                                                                                                                                                                                                                 |
| `failedMaxRetryNum`           | int     | false    | 20                | When append failed, max retry num. Wait 2 seconds for each retry.                                                                                                                                                                                                                |
| `partitionedTables`           | boolean | false    | true              | Create a partitioned table when the table is automatically created. It will use `__event_time__` the partition key.                                                                                                                                                              |
| `partitionedTableIntervalDay` | int     | false    | 7                 | Unit: day, `partitionedTableIntervalDay` is number of days between partitioning of the partitioned table                                                                                                                                                                         |
| `clusteredTables`             | boolean | false    | true              | Create a clusteredTables table when the table is automatically created. It will use `__message_id__` the partition key.                                                                                                                                                          |
| `autoCreateTable`             | boolean | false    | true              | Automatically create table when table does not exist.                                                                                                                                                                                                                            |
| `autoUpdateTable`             | boolean | false    | true              | Automatically update table schema when table schema is incompatible.                                                                                                                                                                                                             |
| `defaultSystemField`          | String  | false    | "" (empty string) | Create system fields when the table is automatically created, separate multiple fields with commas. The supported system fields are: `__schema_version__` , `__partition__` , `__event_time__`, `__publish_time__` , `__message_id__` , `__sequence_id__` , `__producer_name__`. |

> **Note**
>
> The provided Google Cloud credentials must have permissions to access Google Cloud resources. To use the Google BigQuery sink connector, ensure the Google Cloud credentials have the following permissions to Google BigQuery API:
>
> - bigquery.jobs.create
> - bigquery.tables.create
> - bigquery.tables.get
> - bigquery.tables.getData
> - bigquery.tables.list
> - bigquery.tables.update
> - bigquery.tables.updateData
>
> For more information about Google BigQuery API permissions, see [Google Cloud BigQuery API permissions: Access control](https://cloud.google.com/bigquery/docs/access-control).


## Work with Function Worker

You can create a configuration file (JSON or YAML) to set the properties if you use [Pulsar Function Worker](https://pulsar.apache.org/docs/en/functions-worker/) to run connectors in a cluster.

**Example**

* JSON

   ```json
    {
        "name": "google-bigquery-sink",
        "archive": "connectors/pulsar-io-bigquery-{{connector:version}}.jar",
        "className": "org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink",
        "tenant": "public",
        "namespace": "default",
        "inputs": [
          "test-google-bigquery-pulsar"
        ],
        "parallelism": 1,
        "configs": {
          "projectId": "SECRETS",
          "datasetName": "pulsar-io-google-bigquery",
          "tableName": "test-google-bigquery-sink",
          "credentialJsonString": "SECRETS"
      }
    }
    ```

* YAML

    ```yaml
     name: google-bigquery-sink
     archive: 'connectors/pulsar-io-bigquery-{{connector:version}}.jar'
     className: org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink
     tenant: public
     namespace: default
     inputs:
       - test-google-bigquery-pulsar
     parallelism: 1
     configs:
       projectId: SECRETS
       datasetName: pulsar-io-google-bigquery
       tableName: test-google-bigquery-sink
       credentialJsonString: SECRETS
    ```

## Work with Function Mesh
You can create a [CustomResourceDefinitions (CRD)](https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/) to create a Google BigQuery sink connector. Using CRD makes Function Mesh naturally integrate with the Kubernetes ecosystem. For more information about Pulsar sink CRD configurations, see [sink CRD configurations](https://functionmesh.io/docs/connectors/io-crd-config/sink-crd-config).

You can define a CRD file (YAML) to set the properties as below.

```yaml
apiVersion: compute.functionmesh.io/v1alpha1
kind: Sink
metadata:
  name: google-bigquery-sink-sample
spec:
  image: streamnative/pulsar-io-bigquery:{{connector:version}}
  className: org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink
  replicas: 1
  maxReplicas: 1
  input:
    topics: 
      - persistent://public/default/test-google-bigquery-pulsar
  sinkConfig:
     projectId: SECRETS
     datasetName: pulsar-io-google-bigquery
     tableName: test-google-bigquery-sink
     credentialJsonString: SECRETS
  pulsar:
    pulsarConfig: "test-pulsar-sink-config"
  resources:
    limits:
    cpu: "0.2"
    memory: 1.1G
    requests:
    cpu: "0.1"
    memory: 1G
  java:
    jar: connectors/pulsar-io-bigquery-{{connector:version}}.jar
  clusterName: test-pulsar
  autoAck: false
```

# How to use

You can use the Google BigQuery sink connector with Function Worker or Function Mesh.

## Work with Function Worker

> Since the connector uses jar package, build-in running is not supported for the time being.

**Step1: Start Pulsar cluster.**
```
PULSAR_HOME/bin/pulsar standalone
```

**Step2: Run the Google BigQuery sink connector.**
```
PULSAR_HOME/bin/pulsar-admin sinks localrun \
--sink-config-file <google-bigquery-sink-config.yaml>
--archive <pulsar-io-bigquery-{{connector:version}}.jar>
```

Or you can create to on pulsar cluster.
```
PULSAR_HOME/bin/pulsar-admin sinks create \
--sink-config-file <google-bigquery-sink-config.yaml>
--archive <pulsar-io-bigquery-{{connector:version}}.jar>
```

**Step3: Send messages to Pulsar topics.**

This example sends ten “hello” messages to the `test-google-pubsub-pulsar` topic in the `default` namespace of the `public` tenant.

 ```
PULSAR_HOME/bin/pulsar-client produce public/default/test-google-pubsub-pulsar --messages hello -n 10
 ```

**Step4: Query on Google BigQuery**

Please go to Google BigQuery Console to query the data in the table.

## Work with Function Mesh

This example describes how to create a Google BigQuery sink connector for a Kubernetes cluster using Function Mesh.

### Prerequisites

- Create and connect to a [Kubernetes cluster](https://kubernetes.io/).

- Create a [Pulsar cluster](https://pulsar.apache.org/docs/en/kubernetes-helm/) in the Kubernetes cluster.

- [Install the Function Mesh Operator and CRD](https://functionmesh.io/docs/install-function-mesh/) into the Kubernetes cluster.

- Prepare Google BigQuery service. For details, see [Getting Started with Google BigQuery](https://cloud.google.com/bigquery/docs/quickstarts).

### Step

1. Define the Google BigQuery sink connector with a YAML file and save it as `sink-sample.yaml`.

   This example shows how to publish the Google BigQuery sink connector to Function Mesh with a Docker image.

   ```yaml
   apiVersion: compute.functionmesh.io/v1alpha1
   kind: Sink
   metadata:
     name: google-bigquery-sink-sample
   spec:
     image: streamnative/pulsar-io-bigquery:{{connector:version}}
     className: org.apache.pulsar.ecosystem.io.bigquery.BigQuerySink
     replicas: 1
     maxReplicas: 1
     input:
       topics: 
         - persistent://public/default/test-google-bigquery-pulsar
     sinkConfig:
        projectId: SECRETS
        datasetName: pulsar-io-google-bigquery
        tableName: test-google-bigquery-sink
        credentialJsonString: SECRETS
     pulsar:
       pulsarConfig: "test-pulsar-sink-config"
     resources:
       limits:
       cpu: "0.2"
       memory: 1.1G
       requests:
       cpu: "0.1"
       memory: 1G
     java:
       jar: connectors/pulsar-io-bigquery-{{connector:version}}.jar
     clusterName: test-pulsar
     autoAck: false
   ```

2. Apply the YAML file to create the Google BigQuery sink connector.

   **Input**

    ```
    kubectl apply -f <path-to-sink-sample.yaml>
    ```

   **Output**

    ```
    sink.compute.functionmesh.io/google-pubsub-sink-sample created
    ```

3. Check whether the Google BigQuery sink connector is created successfully.

   **Input**

    ```
    kubectl get all
    ```

   **Output**

    ```
    NAME                                         READY   STATUS      RESTARTS   AGE
    pod/google-pubsub-sink-sample-0               1/1    Running     0          77s
    ```

   After that, you can produce and consume messages using the Google BigQuery sink connector between Pulsar and Google BigQuery.