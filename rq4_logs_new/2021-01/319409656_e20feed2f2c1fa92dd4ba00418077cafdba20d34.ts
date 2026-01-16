import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
 
export class LectorService 
{
  constructor(private http:HttpClient) { }

  getRequests(editorId:string) {
    return this.http.get<any>(environment.api + '/publish/all-requests/'+editorId);
  }

  getRequest(requestId: string){
    return this.http.get<any>(environment.api + '/publish/get-request/'+requestId);
  }

  getDocument(url: string) {
		return this.http.get<any>(url);
  }
}