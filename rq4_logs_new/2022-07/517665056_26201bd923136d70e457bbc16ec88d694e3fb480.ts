import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { Adminregister } from '../class/adminregister';

@Injectable({
  providedIn: 'root'
})
export class AdminregisterService {

  private baseUrl='http://localhost:8089/admin/register';

  constructor(private http:HttpClient) { 
  }
  public doRegister(adminregister:Adminregister):Observable<BigInteger>
  {
    return this.http.post<BigInteger>(this.baseUrl,adminregister);
  }
}