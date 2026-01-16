import {Injectable} from '@angular/core';
import {BehaviorSubject, Observable} from 'rxjs';
import {Notification} from '../_models/notification';
import {AuthenticationService} from './authentication.service';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {environment} from '../../environments/environment';
import {map} from 'rxjs/operators';
import {SseService} from './sse.service';

@Injectable({
  providedIn: 'root'
})
export class NotificationsService {
  private notificationsSubject: BehaviorSubject<Notification[]>;
  public notifications: Observable<Notification[]>;

  private notificationsUrl = `${environment.apiUrl}notifications/`;
  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json'
    })
  };

  constructor(private authenticationService: AuthenticationService,
              private http: HttpClient,
              private sseService: SseService) {
    this.notificationsSubject = new BehaviorSubject<Notification[]>([]);
    this.notifications = this.notificationsSubject.asObservable();
    this.sseService.getServerSentEvent(this.authenticationService.currentUserValue.id, 'sent')
      .subscribe(next => this.getUnseen().subscribe());
  }

  getUnseen(): Observable<Notification[]> {
    return this.http.get<Notification[]>(this.notificationsUrl + this.authenticationService.currentUserValue.id, this.httpOptions).pipe(
      map(nots => {
        this.notificationsSubject.next(nots);
        console.log(nots);
        return nots;
      })
    );
  }

  setSeen() {
    this.notificationsSubject.next([]);
    return this.http.post(this.notificationsUrl + 'seen', this.httpOptions);
  }

  sendNot() {
    return this.http.get(this.notificationsUrl + 'new', this.httpOptions);
  }
}