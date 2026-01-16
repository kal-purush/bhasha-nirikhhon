import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import {Headers} from 'angular2/http';

@Injectable({
  providedIn: 'root'
})
export class PostmanService {
  
  constructor(private http: HttpClient) { }
  
  baseUrl = 'https://jsonplaceholder.typicode.com';
  headers = new Headers();



  postSomething(data) {
    this.http.post('http://jsonplaceholder.typicode.com/posts', {
      title: 'foo',
      body: data,
      userId: 1
    })
    .subscribe(
      res => {
        console.log(res);
      },
      err => {
        console.log('Error occured');
      }
  
  };

  // sendToAPI(form: NgForm) {
  //   const formObj = form.value;
  //   const serializedForm = JSON.stringify(formObj);
  //   const req =
  //   this.http.post('http://jsonplaceholder.typicode.com/posts', {
  //     title: 'foo',
  //     body: serializedForm,
  //     userId: 1
  //   })
  //   .subscribe(
  //     res => {
  //       console.log(res);
  //     },
  //     err => {
  //       console.log('Error occured');
  //     }
  //   );
  // }

};