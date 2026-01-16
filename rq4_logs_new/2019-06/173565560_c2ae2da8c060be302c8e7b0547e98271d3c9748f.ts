import {Injectable} from '@angular/core';
import {Url} from '../../environments/environment'
import {Observable} from "rxjs";
import {HttpClient} from "@angular/common/http";
import {UserService} from "./user-service.service";
import {ServicesOffered} from "../models/services-offered";

@Injectable({
  providedIn: 'root'
})
export class WorkerServiceService {

  constructor(private http: HttpClient, private user: UserService) {
  }
  Url = Url + 'worker';
getWorkerService(id):Observable<any>{
  return
}

  getAllservicesForUser(): Observable<ServicesOffered[]> {
    const Id = this.user.CurrentUser._id;
    return this.http.get<ServicesOffered[]>(this.Url + Id);
  }

  getPendingService(): Observable<ServicesOffered[]> {
    const Id = this.user.CurrentUser._id;
    return this.http.get<ServicesOffered[]>(`${this.Url}pending/${Id}`);
  }

  postServices(service: ServicesOffered): Observable<ServicesOffered> {
    return this.http.post<ServicesOffered>(this.Url, service);
  }
}