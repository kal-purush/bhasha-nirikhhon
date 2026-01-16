import { OnEvent, onEventBy } from '@proc7ts/fun-events';
import { CommChannel } from '../comm-channel';
import { CommError } from '../comm-error';
import { CommPacket } from '../comm-packet';
import { CommProcessor } from '../comm-processor';

/**
 * Inbound communication processor, final in processing chain.
 *
 * Its methods raise errors if is not able to process a command.
 */
export class FinalCommProcessor implements CommProcessor {

  readonly #processor: CommProcessor;

  /**
   * Constructs final communication processor.
   *
   * @param processor - Processor to handle inbound commands by.
   */
  constructor(processor: CommProcessor) {
    this.#processor = processor;
  }

  /**
   * Handles received signal.
   *
   * Never skips signal processing
   *
   * @param name - Received signal name.
   * @param signal - Received signal data packet.
   * @param channel - Communication channel the signal received from.
   *
   * @returns Either `true`.
   * @throws CommError  If unknown signal received.
   */
  receive(name: string, signal: CommPacket, channel: CommChannel): true {
    if (!this.#processor.receive(name, signal, channel)) {
      throw new CommError(channel.to, `Unknown signal received: "${name}"`);
    }
    return true;
  }

  /**
   * Responds to request received.
   *
   * Never skips request processing.
   *
   * @param name - Received request name.
   * @param request - Received request data packet.
   * @param channel - Communication channel the request received from.
   *
   * @returns `OnEvent` sender of response data packets. If unknown request received, the response supply wil be cut off
   * with `CommError` as its reason.
   */
  respond(name: string, request: CommPacket, channel: CommChannel): OnEvent<[CommPacket]> {
    return this.#processor.respond(name, request, channel) || onEventBy(({ supply }) => {
      supply.off(new CommError(channel.to, `Unknown request received: "${name}"`));
    });
  }

}