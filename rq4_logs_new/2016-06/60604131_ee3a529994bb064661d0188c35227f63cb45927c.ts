import {Http, Headers} from '@angular/http';
import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/toPromise';

const BASE_URL = 'http://home-accountant.herokuapp.com/api/v1/wallets/';
const HEADER = { headers: new Headers({ 'Content-Type': 'application/json' }) };

export interface Wallet {
  id: number;
  user_id: number;
  category_id: number;
  price: string;
  date: string;
};

@Injectable()
export class WalletService {

  constructor(private http: Http) {}

  loadWallets() {
    return this.http.get(BASE_URL)
      .map(res => res.json())
      .toPromise();
  }

  saveWallet(wallet: Wallet) {
    return (wallet.id) ? this.updateWallet(wallet) : this.createWallet(wallet);
  }

  createWallet(wallet: Wallet) {
    return this.http.post(`${BASE_URL}`, JSON.stringify(wallet), HEADER)
      .map(res => res.json())
      .toPromise();
  }

  updateWallet(wallet: Wallet) {
    return this.http.put(`${BASE_URL}${wallet.id}`, JSON.stringify(wallet), HEADER)
      .map(res => res.json())
      .toPromise();
  }

  deleteWallet(wallet: Wallet) {
    return this.http.delete(`${BASE_URL}${wallet.id}`)
      .map(res => res.json())
      .toPromise();
  }
}