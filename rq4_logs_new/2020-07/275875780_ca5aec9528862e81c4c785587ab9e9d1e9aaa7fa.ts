import { Injectable } from '@angular/core';
import axios from 'axios';


@Injectable({
  providedIn: 'root'
})
export class APIService {
  URL = 'https:localhost:3000/';
  constructor() { }
  getAllTweets(): Promise<any> {
    return axios.get(`${this.URL}tweets`)
      .then(response => response.data)
      .catch(err => console.log(err))
  }
  getAllUsers(): Promise<any> {
    return axios.get(`${this.URL}users`)
      .then(response => response.data)
      .catch(err => console.log(err))
  }
}