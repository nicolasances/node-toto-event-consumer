var kafka = require('kafka-node');
var moment = require('moment-timezone');

var Consumer = kafka.Consumer;
var client = new kafka.KafkaClient({kafkaHost: 'kafka:9092', connectTimeout: 3000, requestTimeout: 6000});


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

    // Start the Kafka consumer
    var options = {groupId: microservice}

    this.consumer = new Consumer(client, [
      {topic: topic}
    ], options);

    this.consumer.on('error', (error) => {
      console.log('Received an error from Kafka Consumer:');
      console.log(error);
      console.log('But I am not stopping!!');
    })

    /**
     * Reacts to receiving a message on the supermarket-categorization topic
     */
    this.consumer.on('message', onMessage);

  }

  /**
   * Returns the registered topics as an [] of topics objects ({topicName, microservice, role})
   */
  getRegisteredTopics() {

    return this.topics;

  }
}

module.exports = TotoEventConsumer;
