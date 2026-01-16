import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class BookService {
  
  constructor(private http:HttpClient) { }

  getBooks(authorId: string) {
    return this.http.get<any>(environment.api + '/book/get/'+authorId);
  }
  
  getBooksFromOtherAuthors(authorId: string) {
	  return this.http.get<any>(environment.api + '/book/get-others/'+authorId);
  }

}