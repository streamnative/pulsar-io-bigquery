#!/usr/bin/env bash

set -e

SRC_DIR=$(git rev-parse --show-toplevel)
(cd "${SRC_DIR}" && mvn clean package -DskipTests)

(cd "${SRC_DIR}/.ci/integrations" && docker-compose up --remove-orphan --build --force-recreate -d)

CONTAINER_NAME="pulsar-io-bigquery-test"
PULSAR_ADMIN="docker exec -d ${CONTAINER_NAME} /pulsar/bin/pulsar-admin"

echo "Waiting for Pulsar service ..."
until curl http://localhost:8080/metrics > /dev/null 2>&1 ; do sleep 1; done
echo "Pulsar service available"

JAR_PATH="/test-pulsar-io-bigquery/pulsar-io-bigquery.jar"

echo "Run sink connector"
SINK_NAME="test-bigquery"
SINK_CONFIG_FILE="/test-pulsar-io-bigquery/test-pulsar-io-bigquery.yaml"
INPUT_TOPIC="test-bigquery-topic"
eval "${PULSAR_ADMIN} sinks localrun -a ${JAR_PATH} \
        --tenant public --namespace default --name ${SINK_NAME} \
        --sink-config-file ${SINK_CONFIG_FILE} \
        -i ${INPUT_TOPIC}"

echo "Waiting for sink and source ..."
sleep 30

echo "Run integration tests"
export GOOGLE_APPLICATION_CREDENTIALS=$SRC_DIR/.ci/integrations/bigquery-key.json
(cd "$SRC_DIR" && mvn -Dtest="*TestIntegration" test -DfailIfNoTests=false)

(cd "${SRC_DIR}/.ci/integrations" && docker-compose down --rmi local)
