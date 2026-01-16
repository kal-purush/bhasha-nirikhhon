declare let window: any;
declare let require: any;

import {Injectable} from '@angular/core';

const Web3 = require('web3');
const contractPath = require('../../../../build/contracts/Gomoku.json');
const contract = require("@truffle/contract");


@Injectable({
  providedIn: 'root'
})
export abstract class AbstractContractService {

  protected account: any = null;
  protected readonly web3: any;
  protected enable: any;

  protected constructor() {
    if (window.ethereum === undefined) {
      alert('Non-Ethereum browser detected. Install MetaMask');
    } else {
      if (typeof window.web3 !== 'undefined') {
        this.web3 = window.web3.currentProvider;
      } else {
        this.web3 = new Web3.providers.HttpProvider('http://127.0.0.1:7545');
      }
      console.log('abstract.contract.service :: constructor :: window.ethereum');
      window.web3 = new Web3(window.ethereum);
      console.log('abstract.contract.service :: constructor :: this.web3');
      console.log(this.web3);
      this.enable = this.enableMetaMaskAccount();
    }
  }

  protected async getAccount(): Promise<any> {
    console.log('abstract.contract.service :: getAccount :: home');
    if (this.account == null) {
      this.account = await new Promise((resolve, reject) => {
        console.log('abstract.contract.service :: getAccount :: eth');
        console.log(window.web3.eth);
        window.web3.eth.getAccounts((err, retAccount) => {
          console.log('abstract.contract.service :: getAccount: retAccount');
          console.log(retAccount);
          if (retAccount.length > 0) {
            this.account = retAccount[0];
            resolve(this.account);
          } else {
            alert('abstract.contract.service :: getAccount :: no accounts found.');
            reject('No accounts found.');
          }
          if (err != null) {
            alert('abstract.contract.service :: getAccount :: error retrieving account');
            reject('Error retrieving account');
          }
        });
      }) as Promise<any>;
    }
    return Promise.resolve(this.account);
  }

  private async enableMetaMaskAccount(): Promise<any> {
    let enable = false;
    await new Promise((resolve, reject) => {
      enable = window.ethereum.enable();
    });
    return Promise.resolve(enable);
  }
}