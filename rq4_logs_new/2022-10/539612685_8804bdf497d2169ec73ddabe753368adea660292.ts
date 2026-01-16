import { Module } from '@nestjs/common';
import { JwtModule } from '@nestjs/jwt';
import { jwtConstants } from './authentication/constants';
import { WebSocketGateway } from 'src/websocket.gateway';

@Module({
  imports: [
    JwtModule.register({
      secret: jwtConstants.secret,
      // signOptions: { expiresIn: 60 },
    }),
  ],
  providers: [WebSocketGateway],
  exports: [WebSocketGateway],
})
export class WebSocketModule {}