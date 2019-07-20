
var TopicCreator = require('./PubSubTopicCreator');

/**
 * This class creates a subscription on PubSub
 */
class SubscriptionCreator {

    /**
     * Constructor
     * @param {PubSub} pubsub Google PubSub instance
     */
    constructor(pubsub) {
        this.pubsub = pubsub;
    }

    /**
     * Subscribes to a topic on PubSub
     * @param {string} topicName name of the topic to subscribe to
     * @param {string} subscriptionName name of the subscription
     * @param {function} onMessage callback to be called when the message is received on the topic
     */
    subscribe(topicName, subscriptionName, onMessage) {

        return new Promise((success, failure) => {

            console.log('Creating subscription [' + subscriptionName + '] on topic [' + topicName + ']...');
    
            this.pubsub.topic(topicName).createSubscription(subscriptionName).then((subscription) => {

                console.log('Subscription [' + subscriptionName + '] created');
        
                // Bind 
                subscription[0].on('message', onMessage);

                console.log('Binding with onMessage function done!');

                success();
        
            }, (err) => {

                // Check the error code: 
                // 6 - The subscription already exists => all fine, get it, bind it and go on.. 
                if (err.code === 6) {

                    // Log
                    console.log('Subscription [' + subscriptionName + '] exists, binding handler...');
            
                    // Get the subscription
                    const subscription = this.pubsub.subscription(subscriptionName);

                    // Bind 
                    subscription.on('message', onMessage);

                    console.log('Binding with onMessage function done!');

                    success();
                }
                // 5 - The topic doesn't exist => create it and retry the whole thing! 
                else if (err.code === 5) {
                    
                    // Log
                    console.log('The topic [' + topicName + '] has not been created. Creating it...');

                    // Create the topic
                    new TopicCreator(this.pubsub).createTopic(topicName).then(() => {

                        console.log('Topic [' + topicName + '] has now been created');

                        // Retry: create the subscription
                        this.subscribe(topicName, subscriptionName, onMessage).then(success, failure);

                    }, failure)

                }
                else failure(err);
        
        
            });

        })
        
    }
    
}

module.exports = SubscriptionCreator;