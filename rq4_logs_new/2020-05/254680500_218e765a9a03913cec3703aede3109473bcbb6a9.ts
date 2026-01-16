import {Injectable} from '@angular/core';
import {Observable} from 'rxjs';
import {HttpClient, HttpHeaders, HttpParams} from '@angular/common/http';
import {Game} from '../_models/game';
import {environment} from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class GameQuestionService {
  url = `${environment.apiUrl}games/`; 
  //url = `http://localhost:8061/api/games/`;
  httpOptions = {
    headers: new HttpHeaders({
      'Content-Type': 'application/json'
    })
  };

  constructor(private http: HttpClient) { }

  getGameData(gameId: string) : Observable<Game>{
    const options = {
      headers: this.httpOptions.headers
    }
    return this.http.get<Game>(this.url + 'game/' + gameId, options);
  }

  savePlayerScore(sid: string, score: number, durTime: number) {
    console.log("Saving score: " + sid + " " + score + " " + durTime);
    const gameinfo = {
      sessionId: sid,
      score: score,
      durationTime: durTime
    }
    return this.http.post(this.url + "result", JSON.stringify(gameinfo), this.httpOptions);
  }
}