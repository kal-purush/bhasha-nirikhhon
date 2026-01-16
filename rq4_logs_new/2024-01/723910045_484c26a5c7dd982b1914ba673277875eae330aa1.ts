/*
 * @Author: zhangxuefeng
 * @Date: 2024-01-15 14:09:16
 * @LastEditors: zhangxuefeng
 * @LastEditTime: 2024-01-15 15:09:09
 * @Description: 
 */
import { Injectable } from '@angular/core';
import { ActivatedRoute, ActivatedRouteSnapshot, Params, Router, UrlSegment } from '@angular/router';
import { BehaviorSubject, Observable } from 'rxjs';

import _ from 'lodash';
import Web3 from 'web3';
import { NzModalService } from 'ng-zorro-antd/modal';



@Injectable({
  providedIn: 'root'
})
export class MetaMaskService {
  public MetaArray$ = new BehaviorSubject<string[]>([]);
  private metaArray: string[] = [];
  web3: any;

  constructor(private router: Router, private activatedRoute: ActivatedRoute,    private modal: NzModalService,) {
    this.web3 = new Web3((window as any).ethereum);
  }
  checkMeataMask(){
    if (!this.web3) {
      this.modal.warning({
        nzTitle: 'Warning',
        nzContent: 'Please install and unlock Metamask first!'
      });
      return;
    }
    this.web3.eth
      .requestAccounts()
      .then(() => this.getAccount())
      .catch(console.error);
  }
  async getAccount(){
    let accounts = await this.web3.eth.getAccounts();
    this.setMetaArray$(accounts);
  }
  getMetaArray$(): Observable<string[]> {
    return this.MetaArray$.asObservable();
  }

  setMetaArray$(tabArray: string[]): void {
    this.MetaArray$.next(tabArray);
  }
  clearMetaArray$():void{
    this.MetaArray$.next([]);
  }
}