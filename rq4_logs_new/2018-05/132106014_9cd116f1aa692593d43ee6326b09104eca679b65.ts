
import { Optional, Injectable } from "@angular/core";
import { Cookie } from "../cookie";
import { CookieService, CookieOptions } from 'ngx-cookie';

@Injectable()
export class CookieBrowser implements Cookie {
  constructor( 
    private _cookieService:CookieService
  ) {}

  get(key: string): string { 
    return this._cookieService.get(key); 
  }

  getAll(): Object { 
    return this._cookieService.getAll(); 
  }

  put(key: string, value: string, options?: CookieOptions) {
    this._cookieService.put(key, value, options); 
  }


  remove(key: string, options?: CookieOptions): void {
    this._cookieService.remove(key, options); 
  }

  removeAll(): void {
    this._cookieService.removeAll(); 
  }
}