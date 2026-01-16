type CallbackAlias = () => void; // () =>  {};  !!!! TS  would consider the object as return value !!!!!

export class Eventing {
  events: { [key: string]: CallbackAlias[] } = {}; // an array of callbacks for a specific event

  on = (eventName: string, callback: CallbackAlias): void => {
    const handlers = this.events[eventName] || [];
    handlers.push(callback);
    this.events[eventName] = handlers;
  }

  trigger = (eventName: string): void => {
    const handlers = this.events[eventName];

    if (!handlers || handlers.length === 0) return;

    handlers.forEach((callback) => callback());
  }
}