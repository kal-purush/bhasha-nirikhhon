import { Injectable } from '@angular/core';
import {Http, Response, Headers, RequestOptions} from '@angular/http';
import {Observable} from 'rxjs/Observable';
import {Article} from './article/article.model';

@Injectable()
export class ArticleService {
  http:Http;
  constructor(http: Http) {
    this.http = http;
   }

  getArticles(): Observable<Article[]>{
    console.log("In getArticles");
    return this.http.request('http://localhost:3000/api/v1/articles')
                    .map(this.extractData)
                    .catch(this.handleError);
  }

  addArticle(title:string, link:string): Observable<Article>{
    let headers = new Headers({'Content-Type': 'application/json'});
    let options = new RequestOptions({headers: headers});

    return this.http.post('http://localhost:3000/api/v1/article', {title, link}, options)
                    .map(this.extractData)
                    .catch(this.handleError);
  }

  updateVotes(id:string, votes:number){
    let headers = new Headers({'Content-Type': 'application/json'});
    let options = new RequestOptions({headers: headers});

    return this.http.put('http://localhost:3000/api/v1/article/'+id, {votes}, options)
                    .map(this.extractData)
                    .catch(this.handleError);
  }

  private extractData(res: Response){
    let body = res.json();
    console.log(body);
    return body || { };
  }

  private handleError(error: Response | any){
    let errMsg: string;
    if(error instanceof Response){
      const body = error.json() || '';
      const err = body.err || JSON.stringify(body);
      errMsg=`${error.status} - ${error.statusText} || ''} ${err}`;

    }
    else{
      errMsg = error.message ? error.message : error.toString();
    }
    console.error(errMsg);
    return Observable.throw(errMsg);
  }
}