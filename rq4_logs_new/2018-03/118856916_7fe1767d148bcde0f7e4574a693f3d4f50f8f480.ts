import { Injectable } from '@angular/core';
import {environment} from "../../environments/environment";

@Injectable()
export class CookieService {

  constructor() { }

  setCookie(cookieName:string,cookieValue:string,expireDays:number) :string {
    var d = new Date();
    d.setTime(d.getTime() + (expireDays*24*60*60*1000));
    var expires = "expires=" + d.toUTCString();
    return document.cookie = cookieName + "=" + cookieValue + ";" + expires + ";path=/;domain="+environment.cookieDomain+';';
  }


  getCookie(cookieName:string): string {
    var name = cookieName + "=";
    var decodedCookie = decodeURIComponent(document.cookie);
    var ca = decodedCookie.split(';');
    for(var i = 0; i < ca.length; i++) {
      var c = ca[i];
      while (c.charAt(0) == ' ') {
        c = c.substring(1);
      }
      if (c.indexOf(name) == 0) {
        return c.substring(name.length, c.length);
      }
    }
    return "";
  }



}