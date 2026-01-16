import { Http, Headers } from '@angular/http';
import { Injectable } from '@angular/core';
import { URLSearchParams } from '@angular/http';
import { MsgToAdmin} from '../_models/MsgToAdmin'
@Injectable()
export class AdminService {
  
   
  
  constructor(private http: Http){}

  public contactAdmin(msg : MsgToAdmin){
    
    let url = "http://localhost:8080/msg";
    let body = JSON.stringify(msg);
    let headers = new Headers();
    console.log(body);
    headers.append('Content-Type', 'application/json');
    return this.http.post(url, body, {headers: headers})
      .map((res) => { 
        return res.json();
       })
  }

public getMsgs() {
    let url = "http://localhost:8080/getmsg";
    let headers = new Headers();
    headers.append('Content-Type', 'application/json');
    return this.http.get(url)
      .map(response => response.json());
  }


}
