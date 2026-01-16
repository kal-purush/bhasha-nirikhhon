import { Injectable, Inject } from '@angular/core';
import { Http } from '@angular/http';
import {
  ConnectionString,
  AccountsInjectionToken,
  ProfilesInjectionToken
} from '../../app/app-config';
import { Storage } from '@ionic/storage';
import 'rxjs/add/operator/toPromise';

@Injectable()
export class AccountServiceProvider {
  constructor(
    public http: Http,
    private storage: Storage,
    @Inject(ProfilesInjectionToken) public profilesApi: ConnectionString,
    @Inject(AccountsInjectionToken) public accountsApi: ConnectionString
  ) {}

  async login(email: string, password: string): Promise<boolean> {
    const loginUrl = this.accountsApi.url + '/login';
    const accountInfo = { email: email, password: password };
    return await this.http
      .post(loginUrl, accountInfo)
      .toPromise()
      .then(res => res.json())
      .then(
        data => {
          this.storage.set('userToken', data.id);
          return true;
        },
        err => {
          this.storage.set('userToken', null);
          return false;
        }
      );
  }

  async logout(): Promise<void> {
    const logoutUrl = this.accountsApi.url + '/logout';
    this.storage
      .get('userToken')
      .then(userToken =>
        this.http.post(logoutUrl, { access_token: userToken })
      );
  }
}