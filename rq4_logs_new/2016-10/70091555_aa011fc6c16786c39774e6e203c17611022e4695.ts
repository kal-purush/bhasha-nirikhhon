import { Injectable } from '@angular/core';
import { Http, Headers } from '@angular/http';
import { environment } from '../../environments/environment';
import { SessionService } from '../session/session.service';

@Injectable()
export class ArApService {

    constructor(private http: Http, private sessionService: SessionService) { }

    getReportData(id, reportType) {
        let headers = new Headers();
        headers.append('Authorization', this.sessionService.headerToken());
        return this.http.get(environment.apiEndpoint + 'api/companies/' + id + '/reports?report_type=' + reportType, {headers: headers});
    }
}