import { Body, Controller, Delete, Get, Param, ParseUUIDPipe, Post, Put } from '@nestjs/common';
import { PeopleService } from './people.service';
import bankManager from './creators/bankManager.model';
  // BankManager endpoints

  @Controller('bankManager')
  export class bankManagerController {
    constructor(private readonly peopleService: PeopleService){}
  @Post()
  createBankManger(
  @Body('name') name: string,
  @Body('cpf') cpf: string,
  ): bankManager {
    return this.peopleService.createBankManager(name,cpf);
  }
  @Get(":id")
  findBankManagerByID(
   @Param('id',new ParseUUIDPipe({ version: '4' })) id: string): bankManager {
  return this.peopleService.findBankManagerById(id)
  }
  @Put(':id')
  updateBankManager(
  @Param('id',new ParseUUIDPipe({ version: '4' })) id: string,
  @Body("name") name: string,
  @Body("cpf") cpf: string,
  ): bankManager{
    return this.peopleService.updateBankManager(id,name,cpf)
    }
 @Delete(":id")
 deactiveBankManager(
    @Param("id",new ParseUUIDPipe({ version: '4' })) id: string
    ):bankManager{
    return this.peopleService.deactivateBankManagerById(id)
    }
}