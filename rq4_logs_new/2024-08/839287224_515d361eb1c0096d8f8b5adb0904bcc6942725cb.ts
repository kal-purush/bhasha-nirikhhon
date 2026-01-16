import { Controller, Delete, Get, Post, Put } from '@nestjs/common';
import { UserService } from './user.service';
// Nhan cac req tu clien -> server va response server -> clienr (data from service)
@Controller('users')
export class UserController {
  constructor(private readonly userService: UserService) {}

  @Get()
  getUsers(): string {
    return this.userService.getUsers();
  }

  @Get('/check-health')
  checkHealth(): string {
    return 'Ok';
  }

  @Post()
  createUser(): string {
    return this.userService.createUser();
  }

  @Get('/:id')
  detailUser(): string {
    return this.userService.detailUser();
  }

  @Put('/:id')
  updateUser(): string {
    return this.userService.updateUser();
  }

  @Delete('/:id')
  deleteUser(): string {
    return this.userService.deleteUser();
  }
}