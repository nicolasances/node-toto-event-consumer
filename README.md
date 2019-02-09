# Toto Event Consumer

This is a npm library to be used to consume from a Kafka topic.

When including this through `require()`, a new `TotoEventConsumer` instance will be automatically created and it will be possible to consume events from kafka.

Typical usage:

```
var TotoEventConsumer = require('toto-event-consumer');
var eventConsumer = new TotoEventConsumer('training-react-session', 'trainingSessionCreated', (event) => {});
```

**NOTE!** that the `event` that is being passed in the callback **has already been parsed and is in JSON format**!
