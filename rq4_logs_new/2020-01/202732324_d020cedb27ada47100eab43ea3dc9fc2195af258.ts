import { HttpErrorResponse } from '@angular/common/http';
import { Observable, of } from 'rxjs';

export class ServerError {
    public code: number;
    public labelNice: string;
    public labelSystem: string;

    constructor(code: number, labelNice: string, labelSystem: string) {
        this.code = code;
        this.labelNice = labelNice;
        this.labelSystem = labelSystem;
    }
}

export class ErrorHandler {

    public static handle(errorObj: HttpErrorResponse): Observable<ServerError> {

        let myreturn: ServerError = null;
        if (errorObj.error instanceof ErrorEvent) {
            myreturn = new ServerError(500, 'Verbindungsproblem', errorObj.message);
        } else {
            myreturn = new ServerError(errorObj.status, 'Verbindungsproblem', errorObj.message);
            if (errorObj.status === 401) {
                myreturn.labelNice = 'Zugriff verweigert - bitte (neu) anmelden!';
            } else if (errorObj.status === 503) {
                myreturn.labelNice = 'Achtung: Server meldet Datenbankproblem.';
            }
        }

        return of(myreturn);
    }
}