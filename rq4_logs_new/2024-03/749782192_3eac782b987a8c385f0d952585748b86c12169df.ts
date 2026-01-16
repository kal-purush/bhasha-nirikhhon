import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Query,
  UseGuards,
} from '@nestjs/common';
import { ChatSerivce } from './chat.service';
import { Member } from '@prisma/client';
import { User } from '../auth/auth.decorator';
import { AuthAdminGuard, AuthMemberGuard } from '../auth/auth.guard';
import { CreateRoomDto } from './dto/create-room.dto';
import { Pagination } from '../../shared/decorators/pagination.decorator';
import { ParseIntWithDefaultPipe } from '../../shared/pagination/pagination.pipe';
import { InviteMemberDto } from './dto/invite-member.dto';
import { ApiTags, ApiOperation } from '@nestjs/swagger';

@ApiTags('채팅 API')
@Controller('chat')
export class ChatController {
  constructor(private readonly chatService: ChatSerivce) {}

  @Get()
  @UseGuards(AuthMemberGuard)
  @ApiOperation({ summary: 'Get the list of chat rooms' })
  async getRoomList(@User() user: Member) {
    const { id } = user;
    return this.chatService.getRoomList(id);
  }

  @Post()
  @UseGuards(AuthMemberGuard)
  @ApiOperation({ summary: 'Create a new chat room' })
  async createRoom(@User() user: Member, @Body() createRoomDto: CreateRoomDto) {
    const { id } = user;
    return this.chatService.createRoom(id, createRoomDto);
  }

  @Get(':roomId')
  @UseGuards(AuthMemberGuard)
  @ApiOperation({ summary: 'Get a specific chat room by ID' })
  async getRoom(@Param('roomId') roomId: string) {
    return this.chatService.getRoom(roomId);
  }

  @Delete(':roomId')
  @UseGuards(AuthAdminGuard)
  @ApiOperation({ summary: 'Delete a specific chat room' })
  async deleteRoom(@Param('roomId') roomId: string) {
    return this.chatService.deleteRoom(roomId);
  }

  @Get(':roomId/messages')
  @UseGuards(AuthMemberGuard)
  @Pagination()
  @ApiOperation({ summary: 'Get the messages of a specific chat room' })
  async getMessages(
    @Param('roomId') roomId: string,
    @Query('cursor', ParseIntWithDefaultPipe) cursor?: number,
    @Query('limit', ParseIntWithDefaultPipe) limit?: number,
  ) {
    return this.chatService.getMessages(roomId, cursor, limit);
  }

  @Post(':roomId/messages/store')
  @UseGuards(AuthAdminGuard)
  @ApiOperation({ summary: 'Store the messages of a specific chat room' })
  async storeMessages(@Param('roomId') roomId: string) {
    return this.chatService.storeMessages(roomId);
  }

  @Post(':roomId/invite')
  @UseGuards(AuthMemberGuard)
  @ApiOperation({ summary: 'Invite a member to a specific chat room' })
  async inviteMember(
    @User() user: Member,
    @Param('roomId') roomId: string,
    @Body() inviteMemberDto: InviteMemberDto,
  ) {
    const { id } = user;
    return this.chatService.inviteMember(id, roomId, inviteMemberDto);
  }

  @Delete(':roomId/leave')
  @UseGuards(AuthMemberGuard)
  @ApiOperation({ summary: 'Leave a specific chat room' })
  async leaveRoom(@User() user: Member, @Param('roomId') roomId: string) {
    const { id } = user;
    return this.chatService.leaveRoom(roomId, id);
  }
}