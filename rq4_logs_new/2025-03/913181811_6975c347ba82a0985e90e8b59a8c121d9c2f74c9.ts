import { Body, Controller, Get, Param, Post } from '@nestjs/common';
import { ConversationService } from './conversation.service';
import { ApiBody, ApiOperation, ApiParam, ApiResponse, ApiSecurity } from '@nestjs/swagger';
import { GetUser } from 'src/decorators/get-user.decorator';
import { CreateConversationDto } from './dto/create-conversation.dto';
import { ConversationsResDto } from './dto/conversations-res.dto';

@Controller('conversation')
@ApiSecurity('token')
export class ConversationController {
  constructor(private conversationService: ConversationService) {}

  @Post('test')
  test() {
    return this.conversationService.test();
  }

  @Post(':videoId')
  @ApiOperation({ summary: 'Create conversation' })
  @ApiParam({ name: 'videoId', type: 'string' })
  @ApiBody({
    type: CreateConversationDto,
    examples: {
      example1: {
        value: {
          question: 'Ý chính của video là gì?',
        },
      },
    },
  })
  @ApiResponse({ status: 201, type: ConversationsResDto })
  createConversation(
    @GetUser('id') userId: string,
    @Param('videoId') videoId: string,
    @Body() createConversationDto: CreateConversationDto,
  ) {
    return this.conversationService.createConversation(
      userId,
      videoId,
      createConversationDto.question,
    );
  }


  @Get()
  @ApiOperation({ summary: 'Get all conversations' })
  @ApiResponse({ status: 200, type: ConversationsResDto })
  getConversations(@GetUser('id') userId: string) {
    return this.conversationService.getConversations(userId);
  }
}