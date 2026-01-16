import { Injectable } from '@angular/core';
import { HttpClient } from '@angular/common/http';
import { Storage } from '@ionic/storage';
import { Tournament } from './models/tournament';

@Injectable({
  providedIn: 'root'
})
export class TournamentService {

  tournaments: Tournament[];

  constructor(private http: HttpClient, private storage: Storage) {
    this.tournaments = [];
   }

  createTournament(payload: object) {

    const tournamentUrl = `api/tournament`;
    return this.http.post(tournamentUrl, payload);
/*     const tournamentUrl = `api/tournament`;
    return this.http.post<Tournament[]>(tournamentUrl, payload).subscribe(tournaments =>{
      console.log(`Tournament created`, tournaments);
      this.tournaments = tournaments; */
  }
}