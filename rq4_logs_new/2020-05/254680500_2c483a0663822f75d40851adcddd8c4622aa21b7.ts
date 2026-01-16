import { Injectable } from '@angular/core';
import { HttpHeaders, HttpClient } from '@angular/common/http';
import { DomSanitizer } from '@angular/platform-browser';
import { HandleErrorsService } from './handle-errors.service';
import { ExtendedQuiz } from '../_models/extended-quiz';
import { Observable } from 'rxjs';
import { map, catchError } from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class ShortQuizListService {

  private baseUrl = 'https://qznetbc.herokuapp.com/api/quizzes/';
  private shortListUrl = 'short-list';

  private info: any;
  private httpOptions = {};

  constructor(private http: HttpClient, private sanitizer: DomSanitizer,
              private handleErrorsService: HandleErrorsService) {
    this.info = JSON.parse(localStorage.getItem('userData'));
    this.httpOptions = {
      headers: new HttpHeaders({
        'Content-Type': 'application/json',
        Authorization: 'Bearer ' + this.info.token
      })
    };
  }

  getShortQuizList(): Observable<ExtendedQuiz[]>{
    console.log(this.baseUrl + this.shortListUrl);
    return this.http.get<ExtendedQuiz[]>(this.baseUrl + this.shortListUrl, this.httpOptions)
      .pipe(map(data => data.map(x => {
        return new ExtendedQuiz().deserialize(x, this.sanitizer);
      }),catchError(this.handleErrorsService.handleError<ExtendedQuiz[]>("getShortQuizList", []))));
  }

}