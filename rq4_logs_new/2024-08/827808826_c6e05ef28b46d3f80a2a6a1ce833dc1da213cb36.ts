import { promises as fs } from 'fs';
import * as path from 'path';
import poupancaAccount from './creators/poupancaAccount.model';
import { Injectable, NotFoundException } from '@nestjs/common';

@Injectable()
export class poupancaAccountJsonFileRepository {
  private readonly filePathPA = path.resolve('src/Accounts/poupancaAccount.json');

  private async readJsonFile(): Promise<poupancaAccount[]> {
    try {
      const data = await fs.readFile(this.filePathPA, 'utf8');
      return JSON.parse(data) as poupancaAccount[];
    } catch (error) {
      if (error.code === 'ENOENT') {
        return []; // Return an empty array if file doesn't exist
      }
      throw error;
    }
  }

  private async writeJsonFile(data: poupancaAccount[]): Promise<void> {
    await fs.writeFile(this.filePathPA, JSON.stringify(data, null, 2), 'utf8');
  }

  async save(account:poupancaAccount): Promise<poupancaAccount> {
    const data = await this.readJsonFile();
    data.push(account);
    await this.writeJsonFile(data);
    return account;
  }

  async findById(id: string): Promise<poupancaAccount> {
    const data = await this.readJsonFile();
    console.log(data)
    const account = data.find(account => account._accountID === id);
    if(!account) {
        throw new NotFoundException(`Poupanca account with ${id} not found`);
      }
    return account 
  }

  async updateById(id: string, amount: number, initDate: string): Promise<poupancaAccount> {
    const data = await this.readJsonFile();
    const index = data.findIndex(account => account._accountID === id);
    if (index === -1) {
       throw new NotFoundException(`Poupanca account with ${id} not found`);
    }
    data[index]._amount = amount 
    data[index]._initDate = initDate

    await this.writeJsonFile(data);
    return data[index];
  }

  async deleteById(id: string): Promise<poupancaAccount> {
    const data = await this.readJsonFile();
    const index = data.findIndex(account => account._accountID === id);

    if(!index) {
        throw new NotFoundException(`Poupanca account with ${id} not found`);
    }
    const account = new poupancaAccount(data[index]._amount,data[index]._initDate)
    account._accountID = data[index]._accountID
    account._accountNumber = data[index]._accountNumber

    account.processDeactivate()

    data[index] = account

    this.writeJsonFile(data);

    return account 
  }
}