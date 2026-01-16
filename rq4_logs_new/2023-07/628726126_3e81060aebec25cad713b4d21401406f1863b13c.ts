import { SubscribeMessage, WebSocketGateway } from '@nestjs/websockets';
import { UseGuards } from '@nestjs/common';

@WebSocketGateway({
    cors: { origin: '*', methods: ['GET', 'POST'] },
    path: '/sockets',
})
export class SocketGateway {
    @UseGuards(WebSocketGateway)
    @SubscribeMessage('action-runner')
    handleActionRunner(client: any, payload: any) {
        client
            .to(`runner${payload.gameSessionId}`)
            .emit('action-runner', payload.action);
    }

    @UseGuards(WebSocketGateway)
    @SubscribeMessage('connect-runner')
    handleConnectRunner(client: any, gameSessionId: string) {
        client.join(`runner${gameSessionId}`);
    }

    @UseGuards(WebSocketGateway)
    @SubscribeMessage('disconnect-runner')
    handleDisconnectRunner(client: any, gameSessionId: string) {
        client.leave(`runner${gameSessionId}`);
    }
}