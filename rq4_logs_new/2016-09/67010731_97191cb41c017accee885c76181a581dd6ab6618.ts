import { Injectable } from '@angular/core';
import { Http } from '@angular/http';

@Injectable()
export class IssueService {

    constructor(private http: Http) { }

    getIssues() {
        return this.http.get('../../assets/issues.json')
        .map(res => res.json());
    }
}