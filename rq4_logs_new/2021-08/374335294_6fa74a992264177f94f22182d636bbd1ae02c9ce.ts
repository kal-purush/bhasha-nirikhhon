import { OnEvent, onEventBy } from '@proc7ts/fun-events';
import { CommChannel } from '../comm-channel';
import { CommHandler, CommReceiver, CommResponder } from '../comm-handler';
import { CommPacket } from '../comm-packet';
import { CommProcessor } from '../comm-processor';

/**
 * Communication processor that handles inbound commands with matching handlers.
 */
export class HandlerCommProcessor implements CommProcessor {

  readonly #receivers = new Map<string, CommReceiver>();
  readonly #responders = new Map<string, CommResponder>();

  /**
   * Constructs handler communication processor.
   *
   * @param handlers - Communication handlers to process inbound commands with.
   */
  constructor(...handlers: CommHandler[]) {
    for (const handler of handlers) {
      if (isCommReceiver(handler)) {
        this.#receivers.set(handler.name, handler);
      } else {
        this.#responders.set(handler.name, handler);
      }
    }
  }

  receive(name: string, signal: CommPacket, channel: CommChannel): void {

    const receiver = this.#receivers.get(name);

    if (!receiver) {
      throw new TypeError(`Unknown signal received: "${name}"`);
    }

    receiver.receive(signal, channel);
  }

  respond(name: string, request: CommPacket, channel: CommChannel): OnEvent<[CommPacket]> {

    const responder = this.#responders.get(name);

    if (!responder) {
      return onEventBy(({ supply }) => {
        supply.off(new TypeError(`Unknown request received: "${name}"`));
      });
    }

    return responder.respond(request, channel);
  }

}

function isCommReceiver(handler: CommHandler): handler is CommReceiver {
  return typeof (handler as Partial<CommReceiver>).receive === 'function';
}