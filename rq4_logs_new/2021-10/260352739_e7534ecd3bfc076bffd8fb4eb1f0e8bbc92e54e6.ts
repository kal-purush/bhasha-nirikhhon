import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { environment } from "../../environments/environment";


@Injectable({
  providedIn: "root"
})
export class SearchService {

  constructor(
    private http: HttpClient
  ) {
  }
  listSearch(query) {
    return this.http.get(`${environment.host}/search/list/${query}`, {
      withCredentials: true
    });
  }
  userSearch(query) {
    return this.http.get(`${environment.host}/search/user/${query}`, {
      withCredentials: true
    });
  }
}