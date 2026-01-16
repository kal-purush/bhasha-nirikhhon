import { Injectable, Inject } from '@angular/core';

import { AuthService } from '../../contracts';
import { HttpClient } from '@angular/common/http';
import { AuthStorageService } from '../auth-storage';

@Injectable()
export class RemoteAuthService extends AuthService {
  get authenticated() {
    return !!this.authStore.getInfo();
  }

  constructor(@Inject('remoteHost') private readonly remoteHost: string,
              private readonly http: HttpClient,
              private authStore: AuthStorageService) {
    super();
  }

  async login(userName: string, password: string) {
    const url = `${this.remoteHost}/auth/login`;

    try {
      const response = await this.http.post<any>(
        url,
        { login: userName, password }
      )
      .toPromise();

      const token = response.token as string;

      this.authStore.setInfo(userName, token);

      return true;
    } catch (err) {
      return false;
    }
  }

  async logout() {
    this.authStore.cleanUp();
  }

  async getUserInfo() {
    if (!this.authenticated) {
      throw new Error('User is not authentiticated.');
    }

    try {
      const url = `${this.remoteHost}/auth/userinfo`;
      return (await this.http.post<any>(url, null).toPromise()).login as string;
    } catch {
      throw new Error('User is not authentiticated.');
    }
  }
}