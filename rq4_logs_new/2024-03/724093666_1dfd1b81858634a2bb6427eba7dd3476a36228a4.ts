import { Inject, Injectable } from '@nestjs/common';
import { ClientProxy } from '@nestjs/microservices';
import { DriverStatusLog } from 'src/entity/driver-status-log.entity';
import {
  DataSource,
  EntitySubscriberInterface,
  EventSubscriber,
  InsertEvent,
} from 'typeorm';

@Injectable()
// @EventSubscriber()
export class DriverStatusLogSubscriber
  implements EntitySubscriberInterface<DriverStatusLog>
{
  constructor(
    dataSource: DataSource,
    @Inject('GATEWAY_SERVICE')
    private readonly gatewayClient: ClientProxy,
  ) {
    dataSource.subscribers.push(this);
  }

  listenTo() {
    return DriverStatusLog;
  }

  /**
   * Called after entity insertion.
   */
  afterInsert(event: InsertEvent<DriverStatusLog>) {
    console.log(`AFTER ENTITY INSERTED: `, event.entity);
    this.gatewayClient.emit('order_updated', {
      order_id: event.entity.order_id,
    });
  }
}