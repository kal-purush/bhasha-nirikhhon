import { Body, Controller, Post, UseGuards } from '@nestjs/common';
import { GetUser } from '../user/user.decorator';
import { UserDto } from '../dto/user.dto';
import { BugService } from './bug.service';
import { JwtGuard } from '../auth/guard/jwt.guard';

@UseGuards(JwtGuard)
@Controller('bug')
export class BugController {
  constructor(private readonly bugService: BugService) {}
  @Post('report')
  async reportBug(
    @GetUser() user: UserDto,
    @Body('title') title: string,
    @Body('body') body: string,
  ) {
    return await this.bugService.reportBug(user.userName, title, body);
  }
}