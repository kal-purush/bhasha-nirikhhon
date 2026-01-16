import { Injectable } from 'angular2/core';
import {Http, Response} from 'angular2/http';
import {Observable}     from 'rxjs/Observable';

import { Student, StudentTrack } from '../models/student';


let STUDENT_TRACK_API_URL = '/api/studenttracks'

@Injectable()
export class StudentTrackService {

    constructor(private http: Http) {
    }

    getStudentTracks(): Observable<StudentTrack[]> {
        return this.http.get(`${STUDENT_TRACK_API_URL}/all.json`)
            .map(res => <any[]>res.json())
            .map(res => res.map(studentTrackJson => new StudentTrack(studentTrackJson)))
             // ?code?
            .catch(this.handleError);
    }
    
    private handleError(error: Response){
        // in a real world app, we may send the error to some remote logging infrastructure
        // instead of just logging it to the console
        console.error(error);
        return Observable.throw(error.json().error || 'Server error');
    }
}