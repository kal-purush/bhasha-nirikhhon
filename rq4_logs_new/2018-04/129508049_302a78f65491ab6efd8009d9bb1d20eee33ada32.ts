import { Http } from '@angular/http';
import { Injectable } from '@angular/core';

@Injectable()
export class DataFinder {

    constructor(private http: Http) {

    }


    public getJSONDataAsync(filePath: string): Promise<any> {
        return new Promise((resolve, reject) => {
            this.http.get(filePath).subscribe(
                response => {
                    if (!response.ok) {
                        reject('Fiailed with status: ' + response.status + '\nTrying to find file at: ' + filePath);
                    }

                    const jsonRes = response.json();

                    resolve(jsonRes);
                }
            );
        }); // .catch((reason) => this.handleError(reason));
    }
}