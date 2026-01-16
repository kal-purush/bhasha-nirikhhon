/**
 * @author Hadi Horani, s144885
 */

import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { BaseService } from './base.service';
import { User } from '../models/login/user.model';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class AdminService extends BaseService {

  constructor(http: HttpClient) {
    super(http);
  }

  fetchUsers() {
    return this.http.get<User[]>(`${this.backendBaseUrl}/api/v1/user`);
  }

  updatehUserRole(id: string, roleName: string) {
    return this.http.put<User>(`${this.backendBaseUrl}/api/v1/id`, {id, roleName});
  }

}