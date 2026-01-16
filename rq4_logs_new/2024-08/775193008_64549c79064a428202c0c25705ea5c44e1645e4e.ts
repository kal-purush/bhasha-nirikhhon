import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class DataService {

  constructor() { }

  public get(key: StorageKeys): string | null {
    return localStorage.getItem(key);
  }

  public set(key: StorageKeys, value: any): void{
    localStorage.setItem(key, value);
  }

  public sessionGet(key: StorageKeys): string | null {
    return sessionStorage.getItem(key);
  }

  public sessionSet(key: StorageKeys, value: any): void{
    sessionStorage.setItem(key, value);
  }

}

export enum StorageKeys {
  "profile_picture" = "PFP_KEY",
  "theme" = "THEME",
}