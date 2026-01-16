import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Observable } from 'rxjs';

import { RegisterSite } from '../_models/registerSite.model'

@Injectable()
export class SiteService {

    constructor(private http: HttpClient) {
    }

    public RegisterNewSite(site: RegisterSite): Observable<boolean> {
        return this.http.post<RegisterSiteResponse>('api/site/', site)
            .map((response: RegisterSiteResponse) => {
                if (response && response.ID) {
                    return true;
                }
                else {
                    return false;
                }
            });
    }
}

export class RegisterSiteResponse {
    constructor(public ID: string) { };
}