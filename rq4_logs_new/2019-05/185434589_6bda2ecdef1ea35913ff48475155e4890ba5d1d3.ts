import { Injectable } from "@angular/core";
import { HttpClient, HttpHeaders } from "@angular/common/http";
import { Observable } from "rxjs";

@Injectable({
  providedIn: "root"
})
export class AdminService {
  private dbUrl = "https://tripn-server.herokuapp.com/";
  constructor(private http: HttpClient) {}

  getUsers(sessionToken): Observable<any> {
    const httpOptions = {
      headers: new HttpHeaders({
        "Content-Type": "application/json",
        Authorization: sessionToken
      })
    };
    return this.http.get(
      "https://tripn-server.herokuapp.com/user/allusers",
      httpOptions
    );
  }
}