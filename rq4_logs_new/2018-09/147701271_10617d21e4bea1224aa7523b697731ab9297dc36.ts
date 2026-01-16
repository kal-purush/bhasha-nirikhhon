import { Get, Controller, Post, Param, Body } from '@nestjs/common';
import { CreateService } from './create.service';
import { CreatePollDto } from './create-poll.dto';
import { Poll } from './poll.entity';
import * as randomstring from 'randomstring';

@Controller('createPoll')
export class CreateController {
  constructor(private readonly createService: CreateService) {}

  @Post()
  async create(@Body() createPollDto: CreatePollDto) {
    let poll = new Poll()
    poll.linkString = randomstring.generate(7);
    poll.question = createPollDto.question;
    poll.answerOne = createPollDto.answerOne;
    poll.answerTwo = createPollDto.answerTwo;
    poll.answerThree = createPollDto.answerThree;  
    poll.answerOneScore = 0;
    poll.answerTwoScore = 0; 
    poll.answerThreeScore = 0; 
    this.createService.create(poll);
  }

  @Get(':linkString')
  async findByLinkString(@Param('linkString') linkString) {
    return this.createService.findByLinkString(linkString)
  }
}