import {
  Controller,
  Get,
  Param,
  Delete,
} from '@nestjs/common';
import { ApiParam, ApiTags } from '@nestjs/swagger';

import { UsersService } from './users.service';

@Controller('users')
@ApiTags('USERS')
export class UsersController {
  constructor(private readonly usersService: UsersService) {}

  @Get()
  getListOfUsers() {
    return this.usersService.getListOfUsers();
  }

  @Get(':id')
  @ApiParam({
    name: 'id',
    example: '1',
  })
  getUserById(@Param('id') id: string) {
    return this.usersService.getUserById(+id);
  }

  @Delete(':id')
  @ApiParam({
    name: 'id',
    example: '1',
  })
  removeUser(@Param('id') id: string) {
    return this.usersService.removeUser(+id);
  }
}