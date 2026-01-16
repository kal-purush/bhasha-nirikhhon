import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { PROLIGA_API, ProligaAPI } from '@data/proliga-api.service';
import { TableRecord } from '@data/schema/table-record';
import { Observable } from 'rxjs';
import { map } from 'rxjs/operators';

import * as _ from 'lodash';

@Injectable({
  providedIn: 'root'
})
export class TableService {
  constructor(
    private http: HttpClient,
    private proligaApi: ProligaAPI
  ) {}

  /**
   * Get all records of matches for table
   */
  getAllRecords(): Observable<TableRecord[]> {
    return this.http
      .get<TableRecord[]>(
        this.proligaApi.currentAPI(PROLIGA_API.VERSION.ONE) +
        ProligaAPI.prepareURI(PROLIGA_API.ENDPOINTS.TABLES.GET_ALL_RECORDS)
      )
      .pipe(
        map( data => this.sortPositionOnTable(data) )
      );
  }

  /**
   * Algorithm for sorting records
   * Primary: Points
   * Secondary: GD
   * Third: GF
   * Final: Club Name
   * @param records
   */
  private sortPositionOnTable(records: TableRecord[]): TableRecord[] {
    const sortedRecords = records.sort(
      (a, b) =>
        ( a.points > b.points ) ? -1 : ( a.points === b.points ) ? // Primary sorting by points
          (
            ( (a.goals_for - a.goals_against) > (b.goals_for - b.goals_against) ) ? -1 : ( (a.goals_for - a.goals_against) === (b.goals_for - b.goals_against) ) ? // Secondary sorting by GD
              (
                ( a.goals_for > b.goals_for ) ? -1 : ( a.goals_for === b.goals_for ) ? // Third sorting by GF
                  (
                    ( a.name < b.name ) ? -1 : 1 // Final sorting by name
                  ) : 1
              ) : 1
          ) : 1
    );

    return sortedRecords;
  }
}