import { Injectable } from '@angular/core';
import { Headers, Http, Response, RequestOptions} from '@angular/http';

import 'rxjs/add/operator/toPromise';

import { Book } from './books-grid/book';

@Injectable()
export class BookService {
  private booksUrl = 'http://localhost:3000/books';  // URL to web api

  constructor(private http: Http) { }

  getBooks(): Promise<Array<Book>> {
    const url = this.booksUrl;

    return this.http
      .get(url)
      .toPromise()
      .then((response) => {
        console.log(response);
        let books: Book[] = [];
        for(let i = 0; i < response.json().data.length; i++){
            books.push(new Book(response.json().data[i].id,
                                response.json().data[i].title,
                                response.json().data[i].author,
                                response.json().data[i].description,
                                response.json().data[i].image));
        }
        return books as Book[];
      })
      .catch(this.handleError);
  }

  loanBook(idbook: number): Promise<boolean> {
    const headers = new Headers({
      'Content-Type': 'application/json',
      'token' : localStorage.id_token
    });

    let url = this.booksUrl + '/' + idbook; 
    return this.http.post(url, JSON.stringify(localStorage.token), { headers: headers })
      .toPromise()
      .then((response) => {
        if(response.json().hasOwnProperty('message')) { return false; }
        return response.json().done as boolean;
      })
      .catch(this.handleError);
  }


  private handleError(error: any): Promise<any> {
    console.error('An error occurred', error);
    return Promise.reject(error.message || error);
  }

}