import {Injectable} from '@angular/core';

import {Target} from '../../interfaces/target.interface';
import {Http, Response} from '@angular/http';
import {environment} from '../../../environments/environment';
import {HttpErrorResponse} from '@angular/common/http';

@Injectable()
export class TargetService {

  constructor(private http: Http) {}

  getTargets(): Promise<Target[]> {
    return this.http.get(environment.serverAddress + '/targets').toPromise().then(
      (res: Response) => res.json(),
      (error: HttpErrorResponse) => console.error('Couldn\'t fetch targets from server.')
    );
  }
}