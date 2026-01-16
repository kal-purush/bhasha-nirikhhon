import { Injectable } from '@angular/core';
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from 'rxjs';
import { catchError } from 'rxjs/operators';
import {ErrorHandler, ServerError} from "./components/iqb-components.module";


@Injectable({
    providedIn: 'root'
})
export class ShowcaseService {

    constructor(private http: HttpClient) { }

    checkError(url: string, parameterName: string, parameterValue: string): Observable<String | ServerError> {
        const headers = new HttpHeaders().set('Content-Type', 'application/json');
        let myBody = {};
        myBody[parameterName] = parameterValue;
        return this.http
            .post<String>(url, myBody, {headers})
            .pipe(
                catchError(ErrorHandler.handle)
            );
    }
}