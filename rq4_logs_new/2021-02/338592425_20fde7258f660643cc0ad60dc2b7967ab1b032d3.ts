import * as http from 'http';
import WebSocket from 'ws';
import { Event, EventType } from './api/models/event.model';

class WebSocketServer {
  private wss: WebSocket.Server;

  constructor(httpServer: http.Server) {
    this.wss = new WebSocket.Server({ server: httpServer });
    this.onConnection();
  }

  get clients(): Set<WebSocket> {
    return this.wss.clients;
  }

  /**
   * Function that sends events to clients.
   * In case of not sending recipient clients,
   * it sends the event to all clients connected to the server.
   *
   * @param event
   * @param clients
   */
  public send(event: Event, clients?: Set<WebSocket> | WebSocket): void {
    const recipients: Set<WebSocket> = this.getRecipients(clients);

    recipients.forEach((client: WebSocket) => {
      client.send(JSON.stringify(event));
    });
  }

  private getRecipients(clients?: Set<WebSocket> | WebSocket): Set<WebSocket> {
    if (!clients) return this.clients;
    if (clients instanceof WebSocket) return new Set<WebSocket>().add(clients);
    else return clients;
  }

  private onConnection() {
    this.wss.on('connection', (ws: WebSocket) => {
      const event: Event = {
        event: EventType.Connection,
        message: 'You have connected to the charge-point-app WebSocket server.'
      };

      this.send(event, ws);
    });
  }
}

export default WebSocketServer;