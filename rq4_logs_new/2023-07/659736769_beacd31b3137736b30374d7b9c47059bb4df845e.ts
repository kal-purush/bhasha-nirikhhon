import {
  Controller,
  Get,
  Post,
  Body,
  Patch,
  Param,
  Delete,
  Res,
} from '@nestjs/common';
import { CreateUserBody } from '../dtos/user/create-user-body';
import { UpdateUserBody } from '../dtos/user/update-user-body';
import { CreateUser } from 'src/application/use-cases/user/create-user';

@Controller('user/')
export class UserController {
  constructor(private createUser: CreateUser) {}

  @Post('create')
  async create(@Body() bodyRequest: CreateUserBody) {
    await this.createUser.execute({ ...bodyRequest });
  }

  // @Get()
  // findAll() {
  //   return 'getAll';
  // }

  // @Get(':id')
  // findById(@Param('id') id: string) {
  //   return 'getById';
  // }

  // @Patch(':id')
  // update(@Param('id') id: string, @Body() updateUserBody: UpdateUserBody) {
  //   return 'update';
  // }

  // @Delete(':id')
  // remove(@Param('id') id: string) {
  //   return 'delete';
  // }
}