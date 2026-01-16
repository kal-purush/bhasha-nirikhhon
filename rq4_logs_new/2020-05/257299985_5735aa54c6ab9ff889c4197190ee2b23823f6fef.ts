import { Injectable } from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';

@Injectable({
  providedIn: 'root'
})
export class ShowroomServiceService {

  public API: string = 'http://localhost:8080/seats';
  public API2: string = 'http://localhost:8080/show-rooms';

  constructor(public http: HttpClient) {
  }

  getAllSeat():Observable<any>{
    return this.http.get(`${this.API}`);
  }
  getAllShowRoom():Observable<any>{
    return this.http.get(`${this.API2}`);
  }

  updateSeat(seat):Observable<any>{
    return this.http.put(`${this.API}/${seat.id}`,seat)
  }

  addNewSeat(seat){
    return this.http.post(this.API,seat);

  }


  addNewShowroom(showroom){
    return this.http.post(this.API2,showroom);

  }
  // getSearchPointAccount(id:number,from:string,to:string,checkPoint:string):Observable<any>{
  //   return this.http.get(`${this.API}/${id}/${from}/${to}/${checkPoint}`);
  // }
}