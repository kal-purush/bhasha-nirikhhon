import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';
import {Observable} from 'rxjs';
import {PokemonDetail} from '../models/pokemon.detail';
import {PokemonList} from '../models/pokemon.list';
import {map} from 'rxjs/operators';
import {environment} from '../../environments/environment';

@Injectable({
  providedIn: 'root'
})
export class PokemonService {
  public baseUrl: any;

  constructor(private http: HttpClient) {
    this.baseUrl = environment.baseUrl;
  }

  getPokemonList(id: number, limit: number): Observable<PokemonList[]> {
    return this.http.get<PokemonList[]>(`${this.baseUrl}?offset=${id}&limit=${limit}`);
  }

  getPokemonDetail(id): Observable<PokemonDetail> {
    return this.http.get<PokemonDetail>(`${this.baseUrl}/${id}`);
  }

}

