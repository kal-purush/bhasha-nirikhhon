import { Module } from '@nestjs/common';
import { ChatgptService } from './services/chatgpt.service';

@Module({
  imports: [],
  providers: [ChatgptService],
  exports: [ChatgptService],
})
export class ChatgptModule {}