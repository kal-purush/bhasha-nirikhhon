import { promises as fs } from 'fs';
import * as path from 'path';
import currentAccount from './creators/currentAccount.model';
import { Injectable, NotFoundException } from '@nestjs/common';

@Injectable()
export class currentAccountJsonFileRepository {
private readonly filePathCC = path.resolve('src/Accounts/currentAccount.json');

  private async readJsonFile(): Promise<currentAccount[]> {
    try {
      const data = await fs.readFile(this.filePathCC, 'utf8');
      return JSON.parse(data) as currentAccount[];
    } catch (error) {
      if (error.code === 'ENOENT') {
        return []; // Return an empty array if file doesn't exist
      }
      throw error;
    }
  }

  private async writeJsonFile(data: currentAccount[]): Promise<void> {
    await fs.writeFile(this.filePathCC, JSON.stringify(data, null, 2), 'utf8');
  }

  async save(account:currentAccount): Promise<currentAccount> {
    const data = await this.readJsonFile();
    data.push(account);
    await this.writeJsonFile(data);
    return account;
  }

  async findById(id: string): Promise<currentAccount> {
    const data = await this.readJsonFile();
    const account = data.find(account => account._accountID === id);
    if(!account) {
        throw new NotFoundException(`Current account with ${id} not found`);
      }
    return account 
  }

  async updateById(id: string, amount: number, initDate: string): Promise<currentAccount> {
    const data = await this.readJsonFile();
    const index = data.findIndex(account => account._accountID === id);
    if (index === -1) {
       throw new NotFoundException(`Current account with ${id} not found`);
    }
    data[index]._amount = amount 
    data[index]._initDate = initDate

    await this.writeJsonFile(data);
    return data[index];
  }

  async deleteById(id: string): Promise<currentAccount> {
    const data = await this.readJsonFile();
    const index = data.findIndex(account => account._accountID === id);

    if(!index) {
        throw new NotFoundException(`Current account with ${id} not found`);
    }
    const account = new currentAccount(data[index]._amount,data[index]._initDate)
    account._accountID = data[index]._accountID
    account._accountNumber = data[index]._accountNumber

    account.processDeactivate()

    data[index] = account

    this.writeJsonFile(data);

    return account 
  }
}