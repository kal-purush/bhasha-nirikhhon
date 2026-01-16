import { CommentDto } from './dto/comment.dto';
import { CommentService } from './comment.service';
import { Post, Body, Controller, UseGuards, Get, Param } from '@nestjs/common';
import { UserDecorator } from '../auth/user/decorators/user.decorator';
import { AuthGuard } from '@nestjs/passport';
import { User } from '../article/interfaces/user.interface';

@Controller('comment')
export class CommentController {
  constructor(private readonly commentService: CommentService) {}

  @Post()
  @UseGuards(AuthGuard('jwt'))
  async create(@UserDecorator() user: User, @Body() commentDto: CommentDto) {
    this.commentService.create({ ...commentDto, author: user._id });
  }

  @Get('/:articleId')
  async findByArticleId(@Param('articleId') articleId: string) {
    return this.commentService.findByArticleId(articleId);
  }
}