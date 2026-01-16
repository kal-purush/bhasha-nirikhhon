export type OscMessage = {
  address: string;
  arguments: [number];
};

export class XAirFeed {
  client: WebSocket;
  subscriptions: {
    [name: string]: {
      callback: (message: OscMessage) => void;
      address?: string;
    };
  } = {};

  constructor(webSocketAddress: string) {
    this.client = new WebSocket(webSocketAddress);
    this.client.onmessage = (resp) => {
      const message = JSON.parse(resp.data) as OscMessage;
      for (var key in this.subscriptions) {
        const { callback, address } = this.subscriptions[key];
        if (!address || message.address === address) {
          callback(message);
        }
      }
    };
  }

  subscribe(callback: (message: OscMessage) => void, address?: string) {
    const name = address || "" + Math.random().toString(36).substring(2);
    this.subscriptions[name] = { callback, address };
    return name;
  }

  unsubscribe(name: string) {
    delete this.subscriptions[name];
  }
}