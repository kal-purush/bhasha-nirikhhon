import { Body, Controller, Get, Post } from '@nestjs/common';
import { ChannelService } from './channel.service';
import { CreateChannelDto } from './dto/create-channel.dto';

@Controller('channel')
export class ChannelController {
  constructor(private readonly channelService: ChannelService) {}

  @Get()
  async getAll() {
    return this.channelService.getAll();
  }

  @Post()
  async create(@Body() channelData: CreateChannelDto) {
    return this.channelService.createChannel(channelData);
  }
}