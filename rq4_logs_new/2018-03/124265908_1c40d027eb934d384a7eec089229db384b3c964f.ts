import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
 
const httpOptions = {
    headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};
 
@Injectable()
export class ReportService {
 
    constructor(private http:HttpClient) {}
 
    getReviews() {
        return this.http.get('http://localhost:3000/api/v1/reviews', {responseType: 'json'});
    }

    postRange(range) {
        let body = JSON.stringify(range);
        return this.http.post('http://localhost:3000/api/v1/reviews', body, httpOptions);
    }

}