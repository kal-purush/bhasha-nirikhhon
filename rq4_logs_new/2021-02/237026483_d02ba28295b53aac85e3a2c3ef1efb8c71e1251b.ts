/* eslint-disable no-dupe-class-members */

import { DefaultTogglingMQClient } from './toggling/DefaultTogglingMQClient';
import { RabbitMQClient } from './rabbitmq/RabbitMQClient';
import { MQClient } from './MQClient';
import { TogglingMQClient } from './TogglingMQClient';

type Config = {
  rabbitMQ: ConstructorParameters<typeof RabbitMQClient>[0];
}

type TogglingConfig = Config & {
  toggling: {
    enabled: boolean;
  };
}

export class MQClientFactory {
  create(config: Config): MQClient;
  create(config: TogglingConfig): TogglingMQClient;
  create(config: any): any {
    const client: MQClient = new RabbitMQClient(config.rabbitMQ);
    if (config.toggling !== undefined) {
      return new DefaultTogglingMQClient({
        enabled: config.toggling.enabled,
        mqClient: client,
      });
    }

    return client;
  }
}