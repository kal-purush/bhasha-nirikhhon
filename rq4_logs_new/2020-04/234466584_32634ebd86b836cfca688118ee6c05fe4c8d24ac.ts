import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import 'rxjs/add/observable/throw';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs/Observable';
import { Researcher } from '../models/researcher.model';

/** This is the service that holds all of the functions to service all the opportunities */
@Injectable()
export class SearchPageService{
    constructor(private http: HttpClient){}
    /** getOpportunities returns all of the opportunities in an array of opportunities */
    getResearchers(input): Observable<Researcher[]>{

        return this.http.get<Researcher[]>('https://localhost:44379/api/researcher?search='+input);
    };
    getResearcher(mnumber): Observable<Researcher>{
        return this.http.get<Researcher>('https://localhost:44379/api/researcher/'+mnumber);
    }
}