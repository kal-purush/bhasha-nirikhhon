import { Injectable, NotFoundException } from '@nestjs/common';
import * as path from 'path';
import * as fs from 'fs';
import Client from './creators/client.model';
import bankManager from './creators/bankManager.model';
import createClientAccount from './products/Client/createClient.model';
import createBankManager from './products/BankManager/createBankManager.model';

@Injectable()
export class PeopleService {
    private readonly filePathBankManger= path.resolve('src/People/bankManager.json');
    private readonly filePathClient = path.resolve('src/People/Client.json');
  
    private readClients(): Client[] {
      const data = fs.readFileSync(this.filePathClient, 'utf8');
      return JSON.parse(data) as Client[]
    }
  
    private writeClients(clients:Client[]): void {
      fs.writeFileSync(this.filePathClient, JSON.stringify(clients, null, 2), 'utf8');
    }
    private readBankManagers(): bankManager[] {
      const data = fs.readFileSync(this.filePathBankManger, 'utf8');
      return JSON.parse(data) as bankManager[]
    }
  
    private writeBankManagers(bankManager: bankManager[]): void {
      fs.writeFileSync(this.filePathBankManger, JSON.stringify(bankManager, null, 2), 'utf8');
    }
  
    createClient(name:string,cpf:string): Client  {
      const clients = this.readClients();
  
      const createClient:createClientAccount = new Client().createAccount()
      const newAccount:Client = createClient.create(name,cpf)
      clients.push(newAccount);
      this.writeClients(clients);
      return newAccount;
    }
  
    findClientById(id: string): Client {
      const clients = this.readClients();
      const client = clients.find(account => account._peopleID === id);
  
      if(!client) {
        throw new NotFoundException(`Client account with ${id} not found`);
      }
  
      return client;
    }
  
    updateClient(id: string, name:string,cpf:string): Client {
      const clientAccounts = this.readClients();
      const clientAccount = clientAccounts.find(account => account._peopleID === id);
  
      if(!clientAccount) {
        throw new NotFoundException(`Current account with ${id} not found`);
      }
      const clientAccountIndex = clientAccounts.findIndex(account => account._peopleID === id);
      clientAccount._name = name;
      clientAccount._cpf = cpf;
  
      clientAccounts[clientAccountIndex] = clientAccount
  
      this.writeClients(clientAccounts);
  
      return clientAccount;
    }
  
    deactivateClientById(id: string): Client {
      const clientAccounts = this.readClients();
      const findClientAccount = clientAccounts.find(account => account._peopleID === id);
      if(!findClientAccount) {
        throw new NotFoundException(`Current account with ${id} not found`);
      }
      const initClient = new Client()
      const accountClient = Object.assign(initClient,findClientAccount) // copy value of properties of findClientAccount to accountClient
      const clientAccountIndex = clientAccounts.findIndex(account => account._peopleID === id)
      const deactivateClient = accountClient.deactivateAccount()
      const account = deactivateClient.deactivate(accountClient)
  
      clientAccounts[clientAccountIndex] = account
  
      this.writeClients(clientAccounts);
      return account
    }
  
    createBankManager(name:string,cpf:string): bankManager {
      const bankManagers = this.readBankManagers();
  
      const createAccount:createBankManager = new bankManager().createAccount()
      const newAccount:bankManager = createAccount.create(name,cpf)
      bankManagers.push(newAccount);
      this.writeBankManagers(bankManagers);
      return newAccount;
    }
  
    findBankManagerById(id: string): bankManager {
      const bankManagers = this.readBankManagers();
      const bankManager = bankManagers.find(account => account._peopleID === id);
  
      if(!bankManager) {
        throw new NotFoundException(`Client account with ${id} not found`);
      }
  
      return bankManager;
    }
  
    updateBankManager(id: string, name:string, cpf:string): bankManager {
      const accounts = this.readBankManagers();
      const bankManager = accounts.find(account => account._peopleID === id);
      const bankManagerIndex = accounts.findIndex(account => account._peopleID === id);
  
      if(!bankManager) {
        throw new NotFoundException(`bank manager with ${id} not found`);
      }
  
      bankManager._name = name;
      bankManager._cpf = cpf;
    
      accounts[bankManagerIndex] = bankManager
  
      this.writeBankManagers(accounts);
  
      return bankManager;
    }
  
    deactivateBankManagertById(id: string): bankManager {
      const bankManagers = this.readBankManagers();
      const findBankManager = bankManagers.find(account => account._peopleID === id);
      if(!findBankManager) {
        throw new NotFoundException(`Bank Manager with ${id} not found`);
      }
      const initBankManager = new bankManager()
      const accountBM = Object.assign(initBankManager,findBankManager) // copy value of properties of findBankManager to bankManager
      const BankMangerIndex = bankManagers.findIndex(account => account._peopleID === id)
      const deactivateAccount = accountBM.deactivateAccount()
      const account = deactivateAccount.deactivate(accountBM)
  
      bankManagers[BankMangerIndex] = account
  
      this.writeBankManagers(bankManagers);
      return account
    }

}
