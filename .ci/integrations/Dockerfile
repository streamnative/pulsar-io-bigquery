FROM apachepulsar/pulsar:2.10.0

COPY .ci/integrations/pulsar-io-bigquery-*.yaml /test-pulsar-io-bigquery/

COPY .ci/integrations/bigquery-key.json /test-pulsar-io-bigquery/bigquery-key.json

ENV GOOGLE_APPLICATION_CREDENTIALS /test-pulsar-io-bigquery/bigquery-key.json

COPY target/pulsar-io-bigquery-*.nar /test-pulsar-io-bigquery/pulsar-io-bigquery.nar

ENTRYPOINT ["/pulsar/bin/pulsar", "standalone"]
