import { Injectable } from 'angular2/core';

import { Artist } from './artist';
import { ARTISTS } from './mock-artists';

@Injectable()
export class ArtistService {

  getArtists(): Promise<Artist[]> {
    return Promise.resolve(ARTISTS);
  }

  getArtistsSlowly(): Promise<Artist[]> {
    return new Promise<Artist[]>(resolve =>
	    setTimeout(resolve, 2000)) // delay 2 seconds
	    .then(() => this.getArtists());
  }

}