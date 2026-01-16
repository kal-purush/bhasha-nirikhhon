import { Injectable } from '@angular/core';
import { Observable, throwError } from 'rxjs';
import { HttpClient, HttpErrorResponse, HttpParams } from '@angular/common/http';
import { map, catchError } from 'rxjs/operators';




@Injectable({
  providedIn: 'root'
})
export class GelocatorService {
  ipAddress:any;
  location:any;
  constructor(private http: HttpClient) { }


  getIpCliente(): Observable<any> {
    this.ipAddress= this.http.get<any>('https://jsonip.com');

return this.http.get<any>('https://jsonip.com');



  }
  getlocation(newName: string): Observable<any> {
    var temp = 'https://geoipify.whoisxmlapi.com/api/v1?apiKey=at_5hXi7N8OgGcRzk6qfCaApUVnndTIN&ipAddress='+newName;//+(hello);
    console.log(newName);
    return this.http.get<any>(temp);
  }


getlocationfromaddress(state: string,address: string): Observable<any>{
var addressFull = "";
var apikey=""
//var temp = "https://maps.googleapis.com/maps/api/geocode/json?address="addresss"&key="apikey;

return this.http.get<any>(apikey);
}



}