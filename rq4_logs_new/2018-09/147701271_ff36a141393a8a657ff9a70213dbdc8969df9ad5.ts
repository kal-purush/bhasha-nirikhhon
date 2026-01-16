//TODO:
// - Make a poll in db
// - Return link to client

import { Get, Controller, Post, Param, Delete, Body } from '@nestjs/common';
import { CreateService } from '../services/create.service';
import { CreatePollDto } from '../dto/create-poll.dto';

@Controller('createPoll')
export class CreateController {
  constructor(private readonly createService: CreateService) {}

  @Post()
  async create(@Body() create: CreatePollDto) {
    this.createService.create(create.toPoll());
  }

  @Get(':id')
  async findOne(@Param('id') id) {
    return this.createService.findOne(id)
  }
}