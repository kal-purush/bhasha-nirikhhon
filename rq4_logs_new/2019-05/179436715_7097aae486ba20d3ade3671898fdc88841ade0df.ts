import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';

@Injectable({
    providedIn: 'root'
})
export class ChangePasswordService {
    readonly API_URL = 'http://127.0.0.1:8000/api';

    constructor(private http: HttpClient) {
    }

    changePassword(data) {
        return this.http.post(`${this.API_URL}/change-password`, data, {headers: this.makeReqHeaderJson()});
    }

    makeReqHeaderJson() {
        const token = localStorage.getItem('token');
        return new HttpHeaders({
            'Content-Type': 'application/json',
            Authorization: 'Bearer ' + token
        });
    }
}