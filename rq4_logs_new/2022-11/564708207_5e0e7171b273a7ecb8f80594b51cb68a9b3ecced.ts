import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import ConversationService from './services/conversation.service';
import MessageService from './services/message.service';
import ConversationController from './controllers/conversation.controller';
import ParticipantController from './controllers/participant.controller';
import ParticipantService from './services/participant.service';
import MessageController from './controllers/message.controller';
import MediaService from './services/media.service';
import MediaController from './controllers/media.controller';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
    }),
  ],
  controllers: [
    ConversationController,
    ParticipantController,
    MessageController,
    MediaController,
  ],
  providers: [
    ConfigService,
    ConversationService,
    MessageService,
    ParticipantService,
    MessageService,
    MediaService,
  ],
})
export class AppModule {}