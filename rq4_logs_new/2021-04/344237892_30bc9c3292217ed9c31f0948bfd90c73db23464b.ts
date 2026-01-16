import { Logger } from 'winston';

import { Publisher } from './publisher.interface';
import { Subscriber } from './subscriber.interface';
import { INotificationMessage } from './subscribers/message.interface';

export class NotificationPublisher implements Publisher {
  private subscribers = new Set<Subscriber>();
  private logger: Logger;

  public constructor(logger: Logger) {
    this.logger = logger;
  }

  public subscribe(subscriber: Subscriber) {
    this.subscribers.add(subscriber);
  }

  public unsubscribe(subscriber: Subscriber) {
    this.subscribers.delete(subscriber);
  }

  public async notify(message: INotificationMessage) {
    try {
      for (const subscriber of this.subscribers) {
        await subscriber.notify(message);
      }
    } catch (error) {
      this.logger.error(error.message);
    }
  }
}