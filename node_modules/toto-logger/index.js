
var moment = require('moment-timezone');


class Logger {

  /**
   * This method logs an incoming call to an API path
   */
  apiIn(correlationId, method, path) {

    let ts = moment().tz('Europe/Rome').format('YYYYMMDDHHmmssSSSSS');

    console.log('[' + ts + '] - [' + correlationId + '] - [api-in] - [info] - Received HTTP call ' + method + ' ' + path);

  }

  /**
   * This method logs an outgoing call to an API
   */
  apiOut(correlationId, microservice, method, path) {

    let ts = moment().tz('Europe/Rome').format('YYYYMMDDHHmmssSSSSS');

    console.log('[' + ts + '] - [' + correlationId + '] - [api-out:' + microservice + '] - [info] - Performing HTTP call ' + method + ' ' + path);

  }

  /**
  * This method logs an incoming message received from Kafka
  */
  eventIn(correlationId, topic) {

    let ts = moment().tz('Europe/Rome').format('YYYYMMDDHHmmssSSSSS');

    console.log('[' + ts + '] - [' + correlationId + '] - [event-in] - [info] - Received event from topic ' + topic);

  }

  /**
  * This method logs an outgoing message sent to a Kafka topic
  */
  eventOut(correlationId, topic) {

    let ts = moment().tz('Europe/Rome').format('YYYYMMDDHHmmssSSSSS');

    console.log('[' + ts + '] - [' + correlationId + '] - [event-out] - [info] - Sending event to topic ' + topic);

  }

  /**
   * This method logs a generic message
   * Log level can be 'info', 'debug', 'error', 'warn'
   */
  compute(correlationId, message, logLevel) {

    let ts = moment().tz('Europe/Rome').format('YYYYMMDDHHmmssSSSSS');

    console.log('[' + ts + '] - [' + correlationId + '] - [compute] - [' + logLevel + '] - ' + message);

  }
}

module.exports = new Logger();
