import { Injectable } from '@angular/core';
import { Observable, of } from 'rxjs';


@Injectable({
    providedIn: 'root'
})
export class DataEntryService {

    constructor() { }

    saveCSV(csv: string): Observable<boolean> {
        const players = this.convertCSVToJS(csv);
        return of(true);
    }

    private convertCSVToJS(csv: string) {
        let rows = csv.split('\n');
        let headers = rows[0].split(',').map(field => field.substring(1, field.length - 1));
        rows.shift();
        let players = [];
        rows.forEach((row) => {
            let splitRow = row.split(',');
            let player = {};

            splitRow.forEach((field, ind) => {
                if (ind > headers.length - 1) {
                    player[headers[headers.length - 1]] = splitRow.slice(headers.length - 1, ind + 1).join();
                } else {
                    player[headers[ind]] = field.substring(1, field.length - 1);
                }
            });
            players.push(player);
        });
        return players;
    }
}