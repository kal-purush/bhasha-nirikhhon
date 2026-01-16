import { EventEmitter, Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { AppConstants } from '../../commons/AppConstants';

@Injectable()
export class SearchService {

  searchResults = new EventEmitter<any>();

  constructor(private http: HttpClient) {
  }

  public searchByTerm(term: string) {
    const {BASE_URL, NAME_URL} = AppConstants.REST_API;
    const url = `${BASE_URL}${NAME_URL}/${term}`;

    this.http
      .get(url)
      .subscribe(data => {
        this.searchResults.emit(data);
      }, error => {
        this.searchResults.emit([]);
      });
  }

  public getCountryDetails(country: string) {
    const {BASE_URL, NAME_URL, FULLNAME_URL} = AppConstants.REST_API;
    const url = `${BASE_URL}${NAME_URL}/${country}/${FULLNAME_URL}`;

    this.http
      .get(url)
      .subscribe(data => {
        this.searchResults.emit(data);
      }, error => {
        this.searchResults.emit([]);
      });
  }

}