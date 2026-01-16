import {
  Body,
  Controller,
  Delete,
  Get,
  Param,
  Post,
  Put,
} from '@nestjs/common';
import { TagDto } from './dot/Tag.dto';
import { TagService } from './tag.service';

@Controller('api')
export class TagController {
  constructor(private tagService: TagService) {}

  @Post('/tag')
  createTag(@Body() tagDto: TagDto) {
    return this.tagService.createTag(tagDto);
  }

  @Get('/tags')
  getTags() {
    return this.tagService.getTags();
  }

  @Put('/tag/:id')
  updateTag(@Param('id') id: number, @Body() tagDto: TagDto) {
    return this.tagService.updateTag(Number(id), tagDto);
  }

  @Delete('/tag/:id')
  deleteTag(@Param('id') id: number) {
    return this.tagService.deleteTag(Number(id));
  }
}