FROM apachepulsar/pulsar:2.10.0

COPY .ci/integrations/test-pulsar-io-bigquery.yaml /test-pulsar-io-bigquery/

COPY target/pulsar-io-bigquery-*.jar /test-pulsar-io-bigquery/pulsar-io-bigquery.jar

ENTRYPOINT ["/pulsar/bin/pulsar", "standalone"]
