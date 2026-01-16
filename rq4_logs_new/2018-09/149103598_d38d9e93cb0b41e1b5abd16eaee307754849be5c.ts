import { Injectable } from '@angular/core';
import { GameDTO } from '../model/GameDTO'; // Dit is onze eigen model
import { HttpClient, HttpHeaders } from '@angular/common/http';
import { Observable } from "rxjs";
import 'rxjs/Rx';
import { gameTitleDTO } from '../model/gameTitleDTO';

const httpOptions = {
  headers: new HttpHeaders({ 'Content-Type': 'application/json' })
};

@Injectable()
export class GameService {

  constructor(private http: HttpClient ) { }

  findGame(title:string): Observable<gameTitleDTO> {
    return this.http.get<gameTitleDTO>('http://localhost:9090/game/scrape/' +title);
  }

  findGames(): Observable<GameDTO[]> {
    return this.http.get<GameDTO[]>('http://localhost:9090/game/all');
  }

}