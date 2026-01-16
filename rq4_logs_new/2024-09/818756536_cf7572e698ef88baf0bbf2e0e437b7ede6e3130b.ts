import { Injectable } from '@angular/core';
import { FavorRequest } from '../../interfaces/favor-request';
import { environment } from '../../../environments/environment';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';
import { Favor } from '../../interfaces/entities/favor';

@Injectable({
  providedIn: 'root'
})
export class FavorService {

  favorRequests: FavorRequest[] = [];
  private baseApiUrl: string = environment.baseApiUrl;
  private apiResourceUrl: string = `${this.baseApiUrl}/favor_request`;

  constructor(private http: HttpClient) { }

  getFavorRequests(): Observable<Favor[]>{
    return this.http.get<Favor[]>(this.apiResourceUrl);
  }

  postFavorRequest(favor: Favor): Observable<Favor> {
    return this.http.post<Favor>(this.apiResourceUrl, favor);
  }

  /*
  async createTalent(talent: Talent): Promise<Talent> {
    const userId = this.userService.getLocalUser()!.id;
    const url = `${this.apiResourceUrl}/user/${userId}`;
    const newTalent = await lastValueFrom(this.http.post<Talent>(url, talent));
    return newTalent;
  }
  */

}