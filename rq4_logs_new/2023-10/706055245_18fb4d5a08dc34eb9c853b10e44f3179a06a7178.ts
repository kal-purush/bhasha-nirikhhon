import {IEventConsumer, IConsumeEvent} from "../interfaces/event-consumer";

/**
 * Custom implementation of the event consumer interface.
 * @implements {IEventConsumer}
 */
export class CustomEventConsumer implements IEventConsumer {
  /**
   * Handle event sent from socket server
   * @param {IConsumeEvent} e Event properties received from EventEmitter
   * @returns {void}
   */
  handleEvent(e: IConsumeEvent): void {
    console.log("write your custom algorithm here");
  }
}