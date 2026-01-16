import {AbstractContractService} from "./abstract.contract.service";
import {Injectable} from '@angular/core';

declare let window: any;
declare let require: any;

const Web3 = require('web3');
const contractPath = require('../../../../build/contracts/Gomoku.json');
const contract = require("@truffle/contract");

export enum FieldState {
  Free,
  White,
  Black
}

class Move {
  address: string;

}

@Injectable({
  providedIn: 'root'
})
export class GameEthereumService extends AbstractContractService {

  gameAddress: string;
  playerName: string;
  playerColour: FieldState;

  constructor() {
    super();
  }

  sendMove(i: number, j: number) {
    this.getAccount();
    const that = this;
    const gomokuContract = contract(contractPath);
    gomokuContract.setProvider(that.web3);
    gomokuContract.deployed().then(instance => {
      instance.play({},
        {
          from: that.account
        });
    });
  }

  proposeDraw(playerName: string, gameAddress: string) {
    this.getAccount();
    const that = this;
    const gomokuContract = contract(contractPath);
    gomokuContract.setProvider(that.web3);
    gomokuContract.deployed().then(instance => {
      instance.joinGame(playerName, gameAddress,
        {
          from: that.account
        });
    });
  }

  bid(playerName: string, gameAddress: string) {
    this.getAccount();
    const that = this;
    const gomokuContract = contract(contractPath);
    gomokuContract.setProvider(that.web3);
    gomokuContract.deployed().then(instance => {
      instance.joinGame(playerName, gameAddress,
        {
          from: that.account
        });
    });
  }
}