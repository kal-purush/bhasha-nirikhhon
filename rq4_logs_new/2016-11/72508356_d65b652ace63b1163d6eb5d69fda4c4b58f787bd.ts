import {GroupConsumer} from "no-kafka";
import {RoundRobinAssignment} from "no-kafka";
import {error} from "./log";
import {Kafka} from "no-kafka";
import {EventEmitter} from "events";

interface Schema {
  (message:any):boolean;
}

interface MessageHandler {
  (message:any, topic:string, partition:number): Promise<void>
}

/**
 * Implementing classes take an ApexFunction and consume messages from the
 * kafka topics for that ApexFunction that match the schemas.
 */
export class SchemaConsumer extends EventEmitter {
  private consumer:GroupConsumer;

  constructor(protected topic:string, protected schemas:Schema[], protected handler:MessageHandler, protected consumerOptions?:Kafka.GroupConsumerOptions) {
    super();

    this.consumer = new GroupConsumer(Object.assign({}, {
      clientId: 'invoker',
      idleTimeout: 100,
      logger: {
        logLevel: 2
      }
    }, consumerOptions));
  }

  private async rawMessageHandler(rawMessage, topic, partition) {
    const offset: Kafka.CommitOffset = {
      topic,
      partition,
      offset: rawMessage.offset
    };

    let message;

    try {
      message = JSON.parse(rawMessage.message.value);
    } catch(err) {
      error('error parsing message on topic', topic, partition);
      error(err);
      error(rawMessage);
      return this.consumer.commitOffset(offset);
    }

    if (this.schemas.length === 0 || this.schemas.every(schema => !schema(message))) {
      return this.consumer.commitOffset(offset);
    }

    await this.handler(message, topic, partition);
    return this.consumer.commitOffset(offset);
  }

  private async dataHandler(rawMessages, topic, partition) {
    return Promise.all(rawMessages.map(rawMessage =>
      this.rawMessageHandler(rawMessage, topic, partition)));
  }

  async init() {
    await this.consumer.init({
      strategy: 'ConsumerStrategy',
      subscriptions: [this.topic],
      fn: RoundRobinAssignment,
      handler: this.dataHandler.bind(this)
    });
  }

  async end() {
    return this.consumer.end();
  }
}
