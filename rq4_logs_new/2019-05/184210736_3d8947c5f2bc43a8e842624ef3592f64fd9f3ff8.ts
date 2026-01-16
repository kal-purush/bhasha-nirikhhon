import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { tap } from 'rxjs/operators';
import { Observable, BehaviorSubject } from  'rxjs';
import { Storage } from '@ionic/storage'

@Injectable({
  providedIn: 'root'
})
export class AuthserviceService {
  AUTH_SERVER_ADRESS: string = 'https://fridge.coredumped.es';
  authSubject = new BehaviorSubject(false);

  constructor(private http: HttpClient, private storage: Storage ) { }

  login(user){
    return this.http.post(`${this.AUTH_SERVER_ADRESS}/login`, user).pipe(
      tap(async (res:{
        token: String,
        isAdmin: Boolean
      }) => {
        if(res.token) {
          await this.storage.set("TOKEN", res.token);
          await this.storage.set("EMAIL", user.email);
          this.authSubject.next(true);
        }
      })
    )
  }
  async isLoggedIn(){
    var resul:Boolean
    await this.storage.get("USER_EMAIL").then((data) => {
      resul = data == null
    })
    return !resul
  }
}