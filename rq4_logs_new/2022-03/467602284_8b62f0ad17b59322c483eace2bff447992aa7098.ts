import { Body, Controller, Get, Post, UseInterceptors } from '@nestjs/common';
import { plainToClass } from 'class-transformer';
import { AccountDTO, createAccountDTO } from '../DTOs/account.dto';
import { AccountService } from './account.service';

@Controller('Account')
export class AccountController {
  constructor(private accountService: AccountService) { }
  
  @Post()
  public async post(@Body() accountDto: createAccountDTO) {
    let res = await this.accountService.create(accountDto);
    return plainToClass(AccountDTO, res);
  }

  @Get()
  public async getAll() {
    let res = await this.accountService.getAll();
    return plainToClass(AccountDTO, res);
  }

}