import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { PROLIGA_API, ProligaAPI } from '@data/proliga-api.service';
import { Season } from '@data/schema/season';
import { ClubList } from '@data/schema/club-list';
import { Observable } from 'rxjs';

/**
 *  Filter Service
 */
@Injectable({
  providedIn: 'root'
})
export class FilterService {

  constructor(
    private http: HttpClient,
    private proligaApi: ProligaAPI
  ) {}

  /**
   * Get All Seasons of a league
   */
  getSeasons(leagueId: number): Observable<Season[]> {
    // Fake Backend Call
    return this.http
      .get<Season[]>(
        this.proligaApi.currentAPI(PROLIGA_API.VERSION.ONE) +
        ProligaAPI.prepareURI(PROLIGA_API.ENDPOINTS.FILTER.GET_SEASONS, {league_id: leagueId})
      );
  }

  getClubs(leagueId: number, seasonId: number) {
    return this.http
      .get<ClubList[]>(
        this.proligaApi.currentAPI(PROLIGA_API.VERSION.ONE) +
        ProligaAPI.prepareURI(PROLIGA_API.ENDPOINTS.FILTER.GET_CLUBS, {league_id: leagueId, season_id: seasonId})
      );

  }
}