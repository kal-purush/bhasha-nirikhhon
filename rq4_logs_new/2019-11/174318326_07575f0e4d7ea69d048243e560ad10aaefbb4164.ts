import { Injectable } from "@angular/core";
import { HttpClient, HttpHeaders, HttpParams} from '@angular/common/http';

import { OAuthService } from 'angular-oauth2-oidc';
import { Observable } from 'rxjs';

import {CampusOnlineHoldings} from '../base/campusonlineholding';
import { environment } from '../../environments/environment';

@Injectable()
export class StudentService {
  private headers;
  private apiURL : string = environment.apiURL;

  constructor (private http: HttpClient, private oauthService: OAuthService) {
    this.headers = {
          headers: new HttpHeaders({
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' +this.oauthService.getAccessToken()
          })
        };
  }

  getStudentListFromCourseonlineholding(course: CampusOnlineHoldings) {
    let url = this.apiURL + "attendance/campusonlineholding/"+course.id+"/?expand=entries.student";
    return this.http.get(url, this.headers);
  }

  checkoutStudentFromCourseOnlineEntrie(id: string) {
    let url = this.apiURL + "attendance/campusonlineentry/" + id + "/transition/";
    console.log(url);
    return this.http.request("DISCARD", url, this.headers);

  }

}