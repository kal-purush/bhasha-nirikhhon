import EventToken, { IEventToken } from './EventToken';

/**
 * A subscribable event.
 */
export interface IEvent<T = {}> {
  /**
   * Subscribes a handler to this event.
   * @param handler - a function that handles the event
   * @returns an event token
   */
  subscribe(handler: (args: T) => void): IEventToken;
  /**
   * Unsubscribes a handler from this event.
   * @param token - the token for the handler
   */
  unsubscribe(token: IEventToken): void;
}

type EventHandler<T> = ((args: T) => void) | undefined;

/**
 * A fireable event.
 */
export default class Event<T = {}> implements IEvent<T> {
  private _handlers: EventHandler<T>[] = [];

  public subscribe(handler: (args: T) => void): IEventToken {
    const openSlot: number = this._handlers.indexOf(undefined);
    if (openSlot >= 0) {
      this._handlers[openSlot] = handler;
      return new EventToken(openSlot, this);
    } else {
      this._handlers.push(handler);
      return new EventToken(this._handlers.length - 1, this);
    }
  }

  public unsubscribe(token: IEventToken): void {
    if (token.id >= 0 && token.id < this._handlers.length) {
      this._handlers[token.id] = undefined;
      token.invalidate();
    }
  }

  /**
   * Fires the event.
   * @param args - the event args
   */
  public raise(args: T): void {
    const currentHandlersCount: number = this._handlers.length;
    for (let i: number = 0; i < currentHandlersCount; ++i) {
      this._handlers[i]?.(args);
    }
  }
}