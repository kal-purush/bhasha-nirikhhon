/// <reference path="../../../typings/angular2/angular2.d.ts" />

import { Injectable } from 'angular2/angular2';
import { Http } from 'angular2/http';

@Injectable()
class SpotifyService {
	url: string;
	
	constructor(public http: Http) {
		this.url = 'https://api.spotify.com/v1/search';
	};
	
	seartchArtist(query) {
		return this.http.get(this.url + '?type=artist&q=' + query);
	};
};

export default SpotifyService;