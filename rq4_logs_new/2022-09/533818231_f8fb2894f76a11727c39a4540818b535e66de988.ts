
import { HttpClient,HttpHeaders } from '@angular/common/http';
import { Injectable } from '@angular/core';
import { Observable } from 'rxjs';
import { environment } from 'src/environments/environment';

@Injectable({
  providedIn: 'root'
})
export class NotificationService {

  constructor(private http:HttpClient) { }

  
  getParcels() : Observable <Notification[]>{


    
    return this.http.get<Notification[]>("http://localhost:8000/api/parcels")
  }


  delete(trackId:string):Observable<{message:string}>{



    return this.http.delete<{message:string}>(`http://localhost:8000/api/parcels/${trackId}`)
  }
  
}