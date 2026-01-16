import { createReceive, createSend, MemuxConfig, Operation } from 'memux';
import * as Config from './config';
import { Doc } from './doc';

export type AnnotatorOptions = {
  name: string;
  receive: (send: (operation: Operation<Doc>) => Promise<void>) => (operation: Operation<Doc>) => Promise<void>;
  customConfig?: typeof Config.Base
};

export async function Annotator({ name, receive, customConfig = {} }: AnnotatorOptions): Promise<void> {
  const config = Object.assign({}, Config.Base, Config.Annotator, customConfig);

  const ssl = {
    key: config.KAFKA_PRIVATE_KEY,
    cert: config.KAFKA_CERT,
    ca: config.KAFKA_CA,
  };

  const send = await createSend({
    name: name || config.NAME,
    url: config.KAFKA_ADDRESS,
    topic: config.OUTPUT_TOPIC,
    concurrency: config.CONCURRENCY,
    ssl
  });

  async function _receive(operation) {
    return receive(send)(operation);
  }

  return createReceive({
    name: name || config.NAME,
    url: config.KAFKA_ADDRESS,
    topic: config.INPUT_TOPIC,
    receive: _receive,
    ssl
  });
}