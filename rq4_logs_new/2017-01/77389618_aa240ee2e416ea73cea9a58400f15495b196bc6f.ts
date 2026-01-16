import { Injectable } 		       from '@angular/core';


@Injectable()
export class LocalStorage {
  public getValue(key, defaultValue){
      return localStorage[key] || defaultValue;
  }

  public setValue(key, value){
    localStorage[key] = value;
    return localStorage[key];
  }

  public get(key){
    return JSON.parse(localStorage[key] || null);
  }

  public set(key, value){
    localStorage[key] = JSON.stringify(value);
    return this.get(key);
  }


}