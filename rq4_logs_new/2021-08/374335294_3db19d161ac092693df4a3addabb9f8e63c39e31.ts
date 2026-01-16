import { CxAsset, CxEntry } from '@proc7ts/context-values';
import { afterThe, mapOn_, OnEvent } from '@proc7ts/fun-events';
import { Logger } from '@proc7ts/logger';
import {
  CommHandler,
  CommPacket,
  commProcessorBy,
  CommProtocol,
  CommResponder,
  MessageCommChannel,
} from '../../communication';
import { Formation } from '../../formation';
import { OrderUnits, Unit } from '../../unit';
import { Formation$Host } from '../formation.host';
import { UseMessagePortCommRequest } from './index';

export class UseMessagePortCommResponder implements CommResponder<UseMessagePortCommRequest> {

  static get entry(): CxEntry<CommProtocol, CommHandler> {
    return CommProtocol;
  }

  static buildAsset(
      target: CxEntry.Target<CommProtocol, CommHandler>,
  ): (collector: CxAsset.Collector<CommHandler>) => void {

    const responder = new UseMessagePortCommResponder(target);

    return collector => collector(responder);
  }

  readonly #host: Formation$Host;
  readonly #orderUnits: OrderUnits;

  private constructor(target: CxEntry.Target<CommProtocol, CommHandler>) {
    this.#host = target.get(Formation$Host);
    this.#orderUnits = this.#host.order.get(OrderUnits);
  }

  get name(): string {
    return UseMessagePortCommRequest;
  }

  respond(request: UseMessagePortCommRequest): OnEvent<[CommPacket]> {
    return afterThe(request).do(
        mapOn_(({ fromFormation, toUnit, port }) => {

          const from = this.#orderUnits.unitByUid(fromFormation, Formation);
          const to = this.#orderUnits.unitByUid(toUnit, Unit);
          const { context } = this.#host.unitDeployment(to);
          const processor = commProcessorBy(context.get(CommProtocol).channelProcessor(from));

          new MessageCommChannel({
            to: from,
            port,
            processor,
            logger: this.#host.context.get(Logger),
          });

          return {};
        }),
    );
  }

}