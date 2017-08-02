# Minimum viable Kafka Graphite

Inspired by dlutzy's excellent [mvredisgraphite](https://github.com/dlutzy/mvredisgraphite) here is mvkg.

Hopefully pretty simple to use, make sure `mvkg.py` and `mvkg.sh` are in the same folder and set the following environment variables:

* `ENVIRONMENT` - basically a prefix 
* `KAFKA_HOST` - the host on which Kafka Jolokia is running
* `KAFKA_PORT` - the port on which Kafka Jolokia is running
* `CARBON_HOST` - the host on which Carbon is running
* `CARBON_PORT` - the port on which Carbon is receiving metrics
* `RANDOM_SLEEP` - an optional setting which will make the script sleep between 1 and `$RANDOM_SLEEP` seconds after retrieving the metrics but before sending them to Carbon (defaults to 0, which is off)
* `OMIT_JVM_STATS` - an optional setting which will cause the script to omit JVM metrics, handy if you are using some other method to get JVM-level metrics into Carbon (defaults to false)

Then run `mvkg.sh` from wherever you've put it. It'll gather the metrics from Jolokia and fire them over to Carbon.

Ensure the KAFKA_JMX_OPTS in your init.d script includes the jolokia agent jar and options "-javaagent:/opt/confluent/confluent-3.2.1/jolokia-jvm-1.2.3-agent.jar=port=9080,host=0.0.0.0"

This will start creating metrics with a prefix of `$ENVIRONMENT.kafka.$HOST`. It's a bit specific to my use-case, any customisations are more than welcome.