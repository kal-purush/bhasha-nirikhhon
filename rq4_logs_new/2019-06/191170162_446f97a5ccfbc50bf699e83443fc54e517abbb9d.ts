import { HttpClient } from '@angular/common/http';
import { PortConfigService } from './port-config.service';
import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class LoginService {

  constructor( private httpClient: HttpClient, private portNumber: PortConfigService ) {}

  login(username: string, password: string) {
      return this.httpClient.post<boolean>('http://localhost:' + this.portNumber.getPort() + '/', {
        'username': username,
        'password': password,
      });
    }
}