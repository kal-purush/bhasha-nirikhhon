import { Controller, Get, Post, Body, Patch, Param, Delete } from '@nestjs/common';
import { JenkinsService } from './jenkins.service';
import { CreateJenkinDto } from './dto/create-jenkin.dto';
// import { UpdateJenkinDto } from './dto/update-jenkin.dto';

@Controller('jenkins')
export class JenkinsController {
  constructor(private readonly jenkinsService: JenkinsService) {}

  @Post()
  create(@Body() createJenkinDto: CreateJenkinDto) {
    return this.jenkinsService.create(createJenkinDto);
  }

  @Get()
  findAll() {
    return this.jenkinsService.findAll();
  }

  @Get(':id')
  findOne(@Param('id') id: string) {
    return this.jenkinsService.findOne(+id);
  }

  // @Patch(':id')
  // update(@Param('id') id: string, @Body() updateJenkinDto: UpdateJenkinDto) {
  //   return this.jenkinsService.update(+id, updateJenkinDto);
  // }

  @Delete(':id')
  remove(@Param('id') id: string) {
    return this.jenkinsService.remove(+id);
  }
}