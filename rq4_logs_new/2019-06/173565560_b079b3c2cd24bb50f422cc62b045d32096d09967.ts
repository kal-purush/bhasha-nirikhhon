import {Injectable} from '@angular/core';
import {Url} from '../../environments/environment';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {Feedback} from '../models/feedback';
import {UserService} from './user-service.service';

@Injectable({
  providedIn: 'root'
})
export class FeedBackService {
  feedUrl = Url + 'feedBack/'

  constructor(private http: HttpClient, private user: UserService) {
  }

  postFeedBack(data: Feedback): Observable<Feedback> {
    let userF =this.user.CurrentUser;
    data.sentBy = userF._id;
    console.log(userF._id);
    return this.http.post<Feedback>(this.feedUrl + 'post', data);
  }

}