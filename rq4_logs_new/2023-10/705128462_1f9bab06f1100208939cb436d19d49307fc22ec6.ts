import { Controller, Get, Param, ParseIntPipe, Query } from '@nestjs/common';
import { CommitsService } from './commits.service';

@Controller('commits')
export class CommitsController {
  constructor(private readonly commitsService: CommitsService) {}

  @Get(':owner/:repo')
  findAll(
    @Param('owner') owner: string,
    @Param('repo') repo: string,
    @Query('page', ParseIntPipe) page: number,
    @Query('perPage', ParseIntPipe) perPage: number
  ) {
    return this.commitsService.findAll({ owner, repo, page, perPage });
  }
}