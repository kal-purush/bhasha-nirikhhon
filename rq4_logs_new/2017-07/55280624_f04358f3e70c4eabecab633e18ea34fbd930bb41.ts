import {Injectable} from '@angular/core';
import {Http, Response, RequestOptions, URLSearchParams} from '@angular/http';
import {Systemunitmap} from './systemunitmap';
import {Observable} from 'rxjs/Rx';
import 'rxjs/add/operator/map';
import 'rxjs/add/operator/catch';
import {APP_SETTINGS} from '../app.settings';

@Injectable()
export class SystemunitmapService {
    constructor (private http: Http) {}

    getSystemunitmap (id: number | string) {
        const options = new RequestOptions({ headers: APP_SETTINGS.MIN_AUTH_JSON_HEADERS });

        return this.http.get(APP_SETTINGS.SYSTEMUNITMAPS_URL + id + '/', options)
            .map(res => <Systemunitmap> res.json())
            .catch(this.handleError);
    }

    getSystemunitmaps (searchArgs?: URLSearchParams) {
        const options = new RequestOptions({ headers: APP_SETTINGS.MIN_AUTH_JSON_HEADERS, search: searchArgs });

        return this.http.get(APP_SETTINGS.SYSTEMUNITMAPS_URL, options)
            .map(res => <Systemunitmap[]> res.json())
            .catch(this.handleError);
    }

    createSystemunitmap (systemunitmap: Systemunitmap): Observable<Systemunitmap> {
        const body = JSON.stringify(systemunitmap);
        const options = new RequestOptions({ headers: APP_SETTINGS.AUTH_JSON_HEADERS });

        return this.http.post(APP_SETTINGS.SYSTEMUNITMAPS_URL, body, options)
            .map(res => <Systemunitmap> res.json())
            .catch(this.handleError)
    }

    updateSystemunitmap (systemunitmap: Systemunitmap): Observable<Systemunitmap> {
        // pull out the ID
        const id = systemunitmap.id;
        delete systemunitmap['id'];

        const body = JSON.stringify(systemunitmap);
        const options = new RequestOptions({ headers: APP_SETTINGS.AUTH_JSON_HEADERS });

        return this.http.put(APP_SETTINGS.SYSTEMUNITMAPS_URL + id + '/', body, options)
            .map(res => <Systemunitmap> res.json())
            .catch(this.handleError)
    }

    private handleError (error: Response) {
        // TODO figure out a better error handler
        // in a real world app, we may send the server to some remote logging infrastructure
        // instead of just logging it to the console
        console.error(error);
        return Observable.throw(error.json().error || 'Server error');
    }
}