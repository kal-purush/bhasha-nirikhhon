import {Injectable} from '@angular/core';

@Injectable()
export class UtilsProvider {

  constructor() {
  }

  tokenBalanceCaclulator(balance: number, decimal: number): number {
    return balance * ((10 ** -decimal));
  }

  tokenValueCalculator(balance: number, decimal: number, price: number): number {
    balance *= ((10 ** -decimal));
    return this.round((balance * price), 2);
  }

  round(number, precision) {
    //source: https://gist.github.com/Bloggerschmidt/37d29cd2b58548b6020f65b5bb3e706e
    var factor = Math.pow(10, precision);
    var tempNumber = number * factor;
    var roundedTempNumber = Math.round(tempNumber);
    return roundedTempNumber / factor;
  }
}