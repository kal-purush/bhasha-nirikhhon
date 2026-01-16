import { Injectable } from "@angular/core";
import { HttpClient } from "@angular/common/http";
import { Observable } from "rxjs";
import { IProfile } from "../modules/dashboard/types/dashboard-state/dashboard-state.interface";
import { ILanguage } from "../modules/dashboard/types/dashboard-state/dashboard-state.interface";
import { IGame } from "../modules/games/types/games-state/games-state.interface";

@Injectable({
  providedIn: "root"
})
export class AppService {
  base_url: "http://localhost:4200";

  constructor(private http: HttpClient) {}

  getProfile(): Observable<IProfile> {
    return this.http.get<IProfile>(`${this.base_url}/profile`);
  }

  getLanguages(): Observable<ILanguage[]> {
    return this.http.get<ILanguage[]>(`${this.base_url}/languages`);
  }

  getGames(): Observable<IGame[]> {
    return this.http.get<IGame[]>(`${this.base_url}/games`);
  }
}