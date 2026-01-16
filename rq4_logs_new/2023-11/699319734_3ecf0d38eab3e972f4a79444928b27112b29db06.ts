import { EventArgs, EventType } from './event-args';
import { EventHandler, Observer } from './observer';

/**
 * Delayed event observer to echo events after the internal game classes have had a chance to respond to them
 * and update their state.
 */
export class DelayedObserver implements Observer {
  private readonly internalObserver: Observer;
  private readonly subscribers: Partial<Record<EventType, EventHandler[]>>;

  /**
   * Instantiates a new DelayedObserver instance to wrap and echo the provided internal observer.
   * @param internalObserver The internal observer to subscribe to and echo.
   */
  constructor(internalObserver: Observer) {
    this.internalObserver = internalObserver;
    this.subscribers = {};

    Object.values(EventType).forEach((type) => {
      internalObserver.subscribe(type, this.onEventReceived.bind(this));
    });
  }

  private onEventReceived(e: EventArgs) {
    setTimeout(() => {
      this.subscribers[e.type]?.forEach((subscriber) => {
        subscriber(e);
      });
    }, 1);
  }

  subscribe(eventType: EventType, handler: EventHandler): void {
    if (!this.subscribers[eventType]) {
      this.subscribers[eventType] = [];
    }

    this.subscribers[eventType]!.push(handler);
  }

  unsubscribe(eventType: EventType, handler: EventHandler): void {
    const index =
      this.subscribers[eventType]?.findIndex((h) => h === handler) ?? -1;

    if (index > -1) {
      this.subscribers[eventType]?.splice(index, 1);
    }
  }
}