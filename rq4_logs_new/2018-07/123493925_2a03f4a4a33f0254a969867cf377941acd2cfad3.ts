import { Injectable } from '@angular/core';
import { Subject } from "rxjs/Subject";
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { TokenManagerService } from './token-manager.service';
import { UserApiService } from "./user-api.service";

@Injectable()
export class OrgApiService {

constructor(private http: HttpClient, private tokenManagerService: TokenManagerService, private userApiService: UserApiService) { }
getOrgInfo() {
  let token = this.tokenManagerService.retrieveToken();
  let bearToken = "bearer " + token;
  let userInfo = this.userApiService.returnUserInfo();
  let userEmail = userInfo.Email;


  return this.http.get("http://millennialsvote.azurewebsites.net/api/users/" + userEmail + "/", {headers: new HttpHeaders({'Authorization': "Bearer " + token})});
}




}