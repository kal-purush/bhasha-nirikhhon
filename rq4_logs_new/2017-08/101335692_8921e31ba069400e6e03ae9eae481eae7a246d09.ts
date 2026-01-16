import { Injectable }    from '@angular/core';
import { Headers, Http } from '@angular/http';

import 'rxjs/add/operator/toPromise';

import { Hero } from '../model/hero';
import { AuthenticationService } from '../service/authentication.service';

@Injectable()
export class HeroService {

    private heroesUrl = 'http://localhost:8080/api';  // URL to web api

    constructor(
        private http: Http,
        private authenticationService: AuthenticationService
    ) {}

getHeroes(): Promise<Hero[]> {
        const url = this.heroesUrl + '/heroes';
        return this.http.get(url, {headers: this.getHeaders()})
            .toPromise()
            .then(response => response.json() as Hero[])
            .catch(this.handleError);
    }


    getHero(id: number): Promise<Hero> {
        const url = this.heroesUrl + '/hero?id=' + id;
        return this.http.get(url, {headers: this.getHeaders()})
            .toPromise()
            .then(response => response.json() as Hero)
            .catch(this.handleError);
    }

    delete(id: number): Promise<void> {
        const url = this.heroesUrl + "/delete/hero?id=" + id;
        return this.http.delete(url, {headers: this.getHeaders()})
            .toPromise()
            .then(() => null)
            .catch(this.handleError);
    }

    create(name: string): Promise<Hero> {
        const url = this.heroesUrl + '/add/hero';
        return this.http
            .post(url, JSON.stringify({name: name}), {headers: this.getHeaders()})
            .toPromise()
            .then(res => res.json())
            .catch(this.handleError);
    }

    update(hero: Hero): Promise<Hero> {
        const url = this.heroesUrl + '/update/hero';
        return this.http
            .put(url, JSON.stringify(hero), {headers: this.getHeaders()})
            .toPromise()
            .then(() => hero)
            .catch(this.handleError);
    }

    private handleError(error: any): Promise<any> {
        console.error('An error occurred', error); // for demo  purposes only
        return Promise.reject(error.message || error);
    }
    
    private getHeaders(){
        // I included these headers because otherwise FireFox
        // will request text/html instead of application/json
        let headers = new Headers({
            'Content-Type': 'application/json',
            'Authorization': 'Bearer ' + this.authenticationService.getToken()
        });
        return headers;
        
        // headers.append('Accept', 'application/json');
    }
}
