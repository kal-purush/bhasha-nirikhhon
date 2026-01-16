import {Injectable} from '@angular/core';
import {HttpClient, HttpParams} from '@angular/common/http';
import {Appointment} from '../../models/appointment.model';
import {Observable} from 'rxjs';

@Injectable()
export class WorkCalFInalService {
  private readonly appointmentsUrl: string;

  constructor(private http: HttpClient) {
    this.appointmentsUrl = 'http://localhost:8080/getAppointments';
  }

  getAppointments(email: string): Observable<Set<Appointment>> {
    return this.http.get<Set<Appointment>>(this.appointmentsUrl + '/' + email + '/');
  }
}