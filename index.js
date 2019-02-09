var kafka = require('kafka-node');
var moment = require('moment-timezone');
var logger = require('toto-logger');

var ConsumerGroup = kafka.ConsumerGroup;


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
    this.consumer = new ConsumerGroup({
      kafkaHost: 'kafka:9092',
      groupId: microservice,
      protocol: ['roundrobin'],
      fromOffset: 'latest',
      commitOffsetsOnFirstJoin: true,
      outOfRangeOffset: 'latest'
    }, topic);

    // React to error messages
    this.consumer.on('error', (error) => {
      console.log('Received an error from Kafka Consumer:');
      console.log(error);
    })

    // React to the offsetOutOfRange
    // This kind of error occurs when for example Kafka deletes the logs (based on the retention policy)
    // and the offset refers to deleted logs
    this.consumer.on('offsetOutOfRange', (error) => {
      console.log('Error: OffsetOutOfRange');
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
