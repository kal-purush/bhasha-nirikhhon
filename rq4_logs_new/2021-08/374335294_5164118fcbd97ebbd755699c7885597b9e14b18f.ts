import { cxDynamic, CxEntry, cxScoped } from '@proc7ts/context-values';
import { Unit, UnitContext } from '../unit';
import { CommHandler, CommReceiver, CommResponder } from './comm-handler';
import { CommProcessor } from './comm-processor';
import { HandlerCommProtocol, ProxyCommProtocol } from './handlers';

/**
 * Per-unit communication protocol for handling inbound commands.
 *
 * Unit's {@link Communicator} handles inbound commands with protocol provided in unit context.
 *
 * Can be constructed out of {@link CommHandler command handlers} as {@link HandlerCommProtocol}.
 */
export interface CommProtocol {

  /**
   * Builds communication processor for commands inbound from the `source` unit.
   *
   * @param source - A unit to process inbound events from.
   *
   * @returns Either inbound signals receiver, request resolver, communication processor, or `undefined` if no inbound
   * commands expected.
   */
  channelProcessor(source: Unit): CommReceiver | CommResponder | CommProcessor | undefined;

}

/**
 * Unit context entry containing communication protocol used by default.
 */
export const CommProtocol: CxEntry<CommProtocol, CommHandler> = {
  perContext: (/*#__PURE__*/ cxScoped(
      UnitContext,
      (/*#__PURE__*/ cxDynamic({
        create(handlers: CommHandler[], _target: CxEntry.Target<CommProtocol, CommHandler>): CommProtocol {
          return new HandlerCommProtocol(...[...handlers].reverse());
        },
        assign({ get, to }) {

          const processor = new ProxyCommProtocol(get);

          return receiver => to((_, by) => receiver(processor, by));
        },
      })),
  )),
  toString: () => '[CommProtocol]',
};