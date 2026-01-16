import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Query,
} from '@nestjs/common';
import { EventsService } from './events.service';
import { EventsDto } from './dto/events.dto';
import { MemberEvent } from '@prisma/client';

@Controller('events')
export class EventsController {
  constructor(private readonly eventsService: EventsService) {}

  @Post()
  async createEvent(@Body() data: EventsDto): Promise<MemberEvent> {
    return await this.eventsService.createEvent(data);
  }

  @Get()
  async getEvents(
    @Query() param: { memberId: number },
  ): Promise<MemberEvent[]> {
    return await this.eventsService.getEvents(Number(param.memberId));
  }

  @Delete(':eventId')
  async deleteEvent(@Param('eventId') eventId: number): Promise<MemberEvent> {
    return await this.eventsService.deleteEvent(Number(eventId));
  }

  @Post(':eventId')
  async updateEvent(
    @Param('eventId') eventId: number,
    @Body() data: EventsDto,
  ): Promise<MemberEvent> {
    return await this.eventsService.updateEvent(Number(eventId), data);
  }
}