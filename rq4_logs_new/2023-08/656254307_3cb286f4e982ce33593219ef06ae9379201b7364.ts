import { Controller, Param, Body, Get, Post, Put, Delete } from 'routing-controllers';

@Controller('/topics')
export class TopicController {
  @Get('/')
  getAll() {
    return 'All topics';
  }

  @Get('/:id')
  getOne(@Param('id') id: number) {
    return 'One topic';
  }

  @Post('/')
  post() {
    return 'Posted topic';
  }

  @Put('/:id')
  put(@Param('id') id: number) {
    return 'Updated topic';
  }

  @Delete('/:id')
  remove(@Param('id') id: number) {
    return 'Deleted topic';
  }
}