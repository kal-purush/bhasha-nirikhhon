import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Item } from '../models/item';
import { Observable } from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ItemService {
  //Instance variables
  private itemsUrl = 'https://rapidscemeteryapi-dev.herokuapp.com/api/item/';
  
  constructor(private http: HttpClient) { }

  /**
   * Method that will use the Angular http client
   * to retrieve data from the API.
   */
  getItems(): Observable<Item[]> {
    return this.http.get<Item[]>(this.itemsUrl+'getAllItems');
  }
}