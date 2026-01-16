import nats, { Stan } from 'node-nats-streaming';

class NatsWrapper {
  private _client?: Stan;

  get client(): Stan {
    if (this._client) return this._client;
    throw new Error('Cannot access NATS client before connecting');
  }

  connect(clusterId: string, clientID: string, url: string) {
    this._client = nats.connect(clusterId, clientID, { url });

    return new Promise((resolve, reject) => {
      this.client.on('connect', () => {
        console.log('Connected to NATS');
        resolve();
      });
      this.client.on('error', (err) => {
        reject(err);
      });
    });
  }
}

export const natsWrapper = new NatsWrapper();