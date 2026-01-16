import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
import { Storage } from "@ionic/storage";
@Injectable()
export class StorageProvider {

  isLoggedIn: boolean;

  constructor(public http: Http, private storage: Storage) {
    console.log('Hello StorageProvider Provider');
  }

  // 存储店铺信息
  saveStoreInfo(data) {
    this.storage.set('storeInfo', JSON.stringify(data));
  }

  // 获取店铺信息
  getStoreInfo() {
    return new Promise(resolve => {
      this.storage.get('storeInfo').then((val)=>{
        // console.log("获得的店铺信息：", val);
        resolve(JSON.parse(val));
      });
    });
  }

  // 保存登录用户的信息
  saveEmployeeInfo(data) {
    this.storage.set('employeeInfo', JSON.stringify(data));
  }

  // 获取登录用户的信息
  getEmployeeInfo() {
    return new Promise(resolve => {
    this.storage.get('employeeInfo').then(val => {
      resolve(JSON.parse(val));
    });
  });
  }

  // 清除登录信息
  changeLoginState(state) {
    this.storage.set("isLoggedIn", state);
  }
}