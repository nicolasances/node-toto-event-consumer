const {PubSub} = require('@google-cloud/pubsub');

var TotoEventConsumer = require('./index.js');
var TopicCreator = require('./util/PubSubTopicCreator');
var SubscriptionCreator = require('./util/PubSubSubscriptionCreator');

var testCreateConsumer = () => {
    var consumer = new TotoEventConsumer('pippo', ['nic-topic-001', 'nic-topic-002'], [(message) => {
        console.log('Received message from Topic 001');
        console.log(message);
    }, (message) => {
        console.log('Received message from Topic 002');
        console.log(message);
    }]);
}

var testCreateTopic = () => {

    new TopicCreator(new PubSub()).createTopic('nic-topic-002');

}

var testCreateSubscription = () => {

    new SubscriptionCreator(new PubSub()).subscribe('nic-topic-003', 'pippolo3', (msg) => {
        console.log(msg);
    });
}

testCreateConsumer();
// testCreateTopic();
// testCreateSubscription();
