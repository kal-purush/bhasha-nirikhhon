import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';

import { Observable } from 'rxjs';

import { Worker } from '../models/worker';
import { environment } from 'src/environments/environment';

@Injectable({
    providedIn: 'root',
})
export class WorkersService {
    constructor(private http: HttpClient) {}
    private apiUrl = environment.API_URL;

    getWorkers(): Observable<Array<Worker>> {
        return this.http.get<Array<Worker>>(`${this.apiUrl}`);
    }

    searchWorker(data: string): Observable<Array<Worker>> {
        const body = { name: data };
        return this.http.post<Array<Worker>>(`${this.apiUrl}`, body);
    }

    deleteWorker(data: string): Observable<Array<Worker>> {
        const body = { del: true, name: data };
        return this.http.post<Array<Worker>>(`${this.apiUrl}`, body);
    }
}