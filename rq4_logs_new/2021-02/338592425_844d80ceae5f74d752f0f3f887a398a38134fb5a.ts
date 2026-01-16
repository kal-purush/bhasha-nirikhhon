import WebSocket from 'ws';
import { IChargePoint } from '../models/charge-point.model';
import { EventType } from '../models/message.model';
import { Notification } from '../models/notification.model';

class NotificationService {
  private wss: WebSocket.Server;

  constructor(wss: WebSocket.Server) {
    this.wss = wss;
  }

  public sendChargePointStatusChange(chargePoint: IChargePoint): void {
    const notification: Notification = {
      event: EventType.ChargePointStatusChange,
      message: `Updated ${chargePoint.name} status to ${chargePoint.status}`,
      data: chargePoint
    };

    this.wss.clients.forEach((client: WebSocket) => {
      client.send(JSON.stringify(notification));
    });
  }
}

export default NotificationService;