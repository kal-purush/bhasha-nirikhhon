import { Inject, Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Rx';

@Injectable()
export class AppConfig {

    private _config: Object = null;

    constructor(private _http: Http) { }

    // Method to get config value by key
    public getConfig(key: any) {
        return this._config[key];
    }

    // Load in config file
    public load() {
        return new Promise((resolve, reject) => {
            let request = this._http.get('config.json');
            request
                .map(res => res.json())
                .catch((error: any) => {
                    console.error('Error reading configuration file');
                    resolve(error);
                    return Observable.throw(error.json().error || 'Server error');
                })
                .subscribe((responseData) => {
                    this._config = responseData;
                    resolve(true);
                });
        });
    };

}