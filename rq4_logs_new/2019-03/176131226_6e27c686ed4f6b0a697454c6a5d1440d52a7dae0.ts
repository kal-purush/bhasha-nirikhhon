import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';


@Injectable({
  providedIn: 'root'
})
export class DataService {

  constructor (private httpClient: HttpClient) {};

  baseUrl = 'http://localhost:3000';
  //http://localhost:3000/
  // getPosts(){
  //   this.httpClient
  //     .get(this.baseUrl + '/posts')
  //     .subscribe((res : any[])=>{
  //       this.data = res;
  //       console.log('data array is', this.data);
  //     })
  // }

  getPosts(){
    return this.httpClient.get(this.baseUrl + '/posts');
  }

  getComments(){
    return this.httpClient.get(this.baseUrl + '/comments');
  }

  getProfile(){
    return this.httpClient.get(this.baseUrl + '/profile');
  }

}