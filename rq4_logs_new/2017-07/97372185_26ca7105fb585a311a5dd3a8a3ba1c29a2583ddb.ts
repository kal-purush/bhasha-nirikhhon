import {Injectable} from '@angular/core';
import {Http, Response} from '@angular/http';
import {environment} from '../../../environments/environment';
import {HttpErrorResponse} from '@angular/common/http';
import {BehaviorSubject} from 'rxjs/BehaviorSubject';
import {Amat} from "../../interfaces/amat.interface";


@Injectable()
export class AmatService {

  amats$: BehaviorSubject<Amat[]> = new BehaviorSubject<Amat[]>([]);

  constructor(private http: Http) {
    this.fetchAmats();
 }

  getAmats$(): BehaviorSubject<Amat[]> {
    return this.amats$;
  }

  fetchAmats() {
    return this.http.get(environment.serverAddress + '/amats').toPromise().then(
      (res: Response) => {
        const data = res.json();

        console.warn('Recieved Amats :', data);
        this.amats$.next(data);
      },
      (error: HttpErrorResponse) => {
        console.error('Couldn\'t fetch targets from server.');
      }
    );
  }
}