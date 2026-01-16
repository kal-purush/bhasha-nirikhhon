import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';

import {map} from 'rxjs/operators';

@Injectable({
  providedIn: 'root'
})
export class HttpSpotifyService {

  constructor(private http: HttpClient) {
  }

  getQuery(query: string) {
    const url = `https://api.spotify.com/v1/${ query }`;

    const headers = new HttpHeaders({
      'Authorization': 'Bearer BQAL94Cd5rm8beKF-mzih_R8Xrz1e5dKCdJ_' +
      'OjgSXbcG7l4csWE3boYKIL' +
      '1oUnLxtZ45lsZAGAg55uszM77' +
      'laMcdTLt8mIeSGB5-C8QoSk2P8AM-5n' +
      '0ivD3k3m1nRwi4-aeE9ZdhQqWdo1lU0nbQ' +
      'Db5bZuhv5nmf37-Oupj6OoyL4fyOLQ'
    });

    return this.http.get(url, {headers});

  }

  getTopTracks(id: string) {
    const result = this.getQuery(`artists/${ id }/top-tracks?country=us`)
      .pipe(map(data => data['tracks']));
    console.log(result);
    return result;
  }

  getArtists( artist: string ) {

    return this.getQuery(`search?q=${ artist }&type=artist&limit=15`)
      .pipe( map( data => data['artists'].items ));

  }
}