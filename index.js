var kafka = require('kafka-node');
var moment = require('moment-timezone');
var logger = require('toto-logger');

var client = new kafka.KafkaClient({
  kafkaHost: 'kafka:9092',
  connectTimeout: 10000,
  requestTimeout: 6000
});

var Consumer = kafka.Consumer;



/**
 * Facade to the Toto event bus publishing functionalities
 */
class TotoEventConsumer {

  /**
   * Constrcutor.
   * Please provide:
   * - microservice   : the name of the microservice (e.g. expenses)
   * - topics         : the name of the topic
   * - onMessage      : the callback to be called (message) => {}
   */
  constructor(microservice, topic, onMessage) {

    // This is the list of topics this consumer is subscribing to
    this.topics = [];

    // Add the topic
    this.topics.push({
      topicName: topic,
      microservice: microservice,
      role: 'consumer'
    });

    // Create the Kafka Consumer Group
    this.consumer = new Consumer(client, [{topic: topic, offset: 'latest', partition: 0}], {groupId: microservice, autoCommit: true, fetchMaxBytes: 100000000, fromOffset: true});

    // React to error messages
    this.consumer.on('error', (error) => {
      console.log('Received an error from Kafka Consumer:');
      console.log(error);
    })

    // React to the offsetOutOfRange
    // This kind of error occurs when for example Kafka deletes the logs (based on the retention policy)
    // and the offset refers to deleted logs
    this.consumer.on('offsetOutOfRange', (error) => {
      console.log('Error: OffsetOutOfRange for topics ' + this.topics[0].topicName);

      var offset = new kafka.Offset(client);

      offset.fetch([{ topic: this.topics[0].topicName, partition: 0, time: -1, maxNum: 1 }], (err, data) => {

        let currentOff = data[this.topics[0].topicName]['0'][0];

        this.consumer.pause();
        this.consumer.setOffset(this.topics[0].topicName, 0, currentOff);
        this.consumer.commit((err, data) => {if (err) console.log(err);});
        this.consumer.resume();

      });
    })

    /**
     * Reacts to receiving a message on the supermarket-categorization topic
     */
    this.consumer.on('message', (message) => {

      if (message == null || message.value == null) {
        console.log("Error: message or message value was null..");
        console.log(message);
      };

      try {

        // 0. Parse the message to get the correlation id
        let eventData = JSON.parse(message.value);

        // 1. Log
        if (eventData.correlationId) logger.eventIn(eventData.correlationId, topic);

        // 2. Provide event to the callback
        onMessage(eventData);

      } catch (e) {

        console.log(e);

      }
    });

  }

  /**
   * Returns the registered topics as an [] of topics objects ({topicName, microservice, role})
   */
  getRegisteredTopics() {

    return this.topics;

  }
}

module.exports = TotoEventConsumer;
