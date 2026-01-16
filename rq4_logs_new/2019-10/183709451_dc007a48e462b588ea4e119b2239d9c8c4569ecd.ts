import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Resolve, ActivatedRouteSnapshot, RouterStateSnapshot} from '@angular/router';
import {Observable, forkJoin} from 'rxjs';
import {ProligaAPI, PROLIGA_API} from './proliga-api.service';

import {ClubModel} from '../models/club.model';

@Injectable()
export class ClubTableService implements Resolve<any> {
  constructor(
    private http: HttpClient,
    private proligaApi: ProligaAPI
  ) {}

  public resolve(route: ActivatedRouteSnapshot, rstate: RouterStateSnapshot): Observable<any> {
    console.log('Logging collected route Parameter', route.params);
    const loadedResults = forkJoin([
      this.getAllClubs()
    ]);

    return loadedResults;
  }

  private getAllClubs(): Observable<ClubModel[]> {
    console.log('get all clubs');
    // Fake Backend Call
    return this.http
      .get<any>(
        this.proligaApi.currentAPI() + ProligaAPI.prepareURI(PROLIGA_API.ENDPOINTS.CLUBS.GET_ALL_CLUBS)
      );
  }
}