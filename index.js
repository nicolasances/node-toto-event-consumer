const {PubSub} = require('@google-cloud/pubsub');
var moment = require('moment-timezone');
var logger = require('toto-logger');

var SubscriptionCreator = require('./util/PubSubSubscriptionCreator');

const pubsub = new PubSub({projectId: 'toto-events-' + process.env.TOTO_ENV});
const subscriptionCreator = new SubscriptionCreator(pubsub);

/**
 * Facade to the Toto event bus publishing functionalities
 */
class TotoEventConsumer {

  /**
   * Constrcutor.
   * Please provide:
   * - microservice   : the name of the microservice (e.g. expenses)
   * - topic          : the name of the topic (if single) or an array of topics
   * - onMessage      : the callback to be called (message) => {} or an array of callbacks (same index as the topics array)
   */
  constructor(microservice, topic, onMessage) {

    // This is the list of topics this consumer is subscribing to
    this.topics = [];

    /**
     * This method prepares the message before serving it to the handler in the 
     * subscribing microservice
     * Structure of "message":
     * {
        topic: 'expensesUploadConfirmed',
        value: '...',
        offset: 0,
        partition: 0,
        highWaterOffset: 1,
        key: null,
        timestamp: 2019-05-25T11:47:25.861Z
      }
     * @param {message} message 
     */
    var prepareMessage = (message) => {

      // 1. Get the subscription from the message 
      let subscriptionFullName = message._subscriber._subscription.name;
      let subscriptionFullNameComp = subscriptionFullName.split('/');
      let subscriptionName = subscriptionFullNameComp[subscriptionFullNameComp.length - 1];

      // 2. Parse the message
      let eventData = JSON.parse(message.data);

      // 3. Find the subscription
      for (var i = 0; i < this.topics.length; i++) {
        if (this.topics[i].subscriptionName === subscriptionName) {

          // 3.1. Log
          if (eventData.correlationId) logger.eventIn(eventData.correlationId, this.topics[i].topicName, eventData.msgId);

          // 3.2. Provide event to the callback
          this.topics[i].onMessage(eventData);

          // 3.3. Ack 
          message.ack();
          
        }
      }

    }

    // Add the topic
    if (Array.isArray(topic)) {
      for (var i = 0; i < topic.length; i++) {
        this.topics.push({
          topicName: topic[i],
          microservice: microservice,
          role: 'consumer',
          onMessage: onMessage[i],
          // The subscription is made as a concatenation of the topic and the microservice name 
          subscriptionName: topic[i] + '-' + microservice
        });
      }
    }
    else this.topics.push({
      topicName: topic,
      microservice: microservice,
      role: 'consumer',
      onMessage: onMessage,
      // The subscription is made as a concatenation of the topic and the microservice name 
      subscriptionName: topic + '-' + microservice
    });

    // Create the Subscription(s) based on how many topics you're listening to
    for (var i = 0; i < this.topics.length; i++) {

      let top = this.topics[i];

      // Create the subscription => subscribe to the topic
      subscriptionCreator.subscribe(top.topicName, top.subscriptionName, prepareMessage);
      
    }

  }

  /**
   * Returns the registered topics as an [] of topics objects ({topicName, microservice, role})
   */
  getRegisteredTopics() {

    return this.topics;

  }
}

module.exports = TotoEventConsumer;
