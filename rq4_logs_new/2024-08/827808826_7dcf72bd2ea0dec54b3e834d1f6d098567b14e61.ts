import { Body, Controller, Delete, Get, NotFoundException, Param, ParseUUIDPipe, Post, Put } from '@nestjs/common';
import poupancaAccount from './creators/poupancaAccount.model';
import openPoupancaAccount from './products/poupancaAccount/openPoupancaAccount.model';
import { AccountsService } from './accounts.service';


@Controller("poupancaAccounts")
export class poupancaAccountController {
  constructor(private readonly accountService: AccountsService){}
    // poupanca Accounts Endpoints
    @Post()
    createPoupancaAccount(
    @Body('isBankManager') isBankManager: boolean,
    @Body('amount') amount: number,
    @Body("accountNumber") accountNumber: string,
    @Body("initDate") initDate: string
    ): poupancaAccount {
    return this.accountService.createPoupancaAccount(isBankManager,accountNumber,amount,initDate);
  }
    @Get(":id")
    findpoupancaAccountByID(
      @Param('id',new ParseUUIDPipe({ version: '4' })) id: string): poupancaAccount {
      return this.accountService.findPoupancaAccountById(id)
    }
    @Put(':id')
    updatepoupancaAccountById(
      @Param('id',new ParseUUIDPipe({ version: '4' })) id: string,
      @Body("accountNumber") accountNumber: string,
      @Body("initDate") initDate: string,
      @Body("amount") amount: number
    ): poupancaAccount{
      return this.accountService.updatePoupancaAccountById(id,amount,initDate,accountNumber)
    }
    @Delete(":id")
    deactivateClientByID(
      @Param("id",new ParseUUIDPipe({ version: '4' })) id: string
    ):poupancaAccount{
      return this.accountService.deactivatePoupancaAccountById(id)
    }
}