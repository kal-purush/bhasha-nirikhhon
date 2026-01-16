import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Mission } from '../models/mission';
import { Observable, of } from 'rxjs';
import { TEST_MISSIONS } from '../test-missions';

@Injectable({
  providedIn: 'root'
})
export class MissionService {
  private heroUrl = 'http://ec2-54-158-218-76.compute-1.amazonaws.com:8085/RevatureHeroes/';

  constructor(private http: HttpClient) { }

  getMissions(id: number): Observable<Mission[]> {
    console.log("post data: ", id)
    return this.http.post<Mission[]>(`${this.heroUrl}getMissions`, id);
  }


  startMission(mission: Mission): Observable<Mission[]> {
    console.log("post data: ", mission)
    return this.http.post<Mission[]>(`${this.heroUrl}startMissions`, mission);
  }


  completeMission(mission: Mission): Observable<Mission[]> {
    console.log("post data: ", mission)
    return this.http.post<Mission[]>(`${this.heroUrl}completeMission`, mission);
  }
  

  getTestMissions(): Observable<Mission[]> {
    return of(TEST_MISSIONS);
  }
}

  //we on the front end need to check if mission is completed
  //we also need to set a timestamp for mission start