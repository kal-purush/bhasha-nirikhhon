import { Observable } from 'rxjs';
import { RestDataSource } from './rest.datasource';
import { Injectable, INJECTOR } from '@angular/core';

@Injectable()

export class AuthService {
    constructor(private ds: RestDataSource) {
    }

    authenticate( username: string, password: string): Observable<boolean> {
        return this.ds.authenticate(username, password);
    }

    // Getter methods to return authenticated property 
    get authenticated(): boolean {
        return this.ds.authToken != null ;
    }

    // clear the authentiated token
    clear() {
        this.ds.authToken = null;
    }

}