
/**
 * This class has the responsibility to create topics on PubSub
 */
class TopicCreator {

    /**
     * Constructor
     * @param {PubSub} pubsub instance
     */
    constructor(pubsub) {
        this.pubsub = pubsub;
    }

    /**
     * This method will create a new topic on the current Google Project (set as an ENV variable)
     */
    createTopic(name) {

        return new Promise((success, failure) => {

            this.pubsub.createTopic(name).then(success, (err) => {

                // In case of error, check the code:
                // Code 6 = topic already exists
                if (err.code === 6) success();
                else failure(err);

            })

        })

    }

}

module.exports = TopicCreator;