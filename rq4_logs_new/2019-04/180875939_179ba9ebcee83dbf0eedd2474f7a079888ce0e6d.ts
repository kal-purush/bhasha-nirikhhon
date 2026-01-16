import {Injectable} from '@angular/core';
import {HttpClient} from '@angular/common/http';

@Injectable() // needed as we're injecting Http service into this service
export class FlickrService {

  key = 'e232a3275cbdf250596b676a9f84fb7d';
  secret = 'fbef367ebcfbb5b0';
  urlBase = 'https://api.flickr.com/services/rest/?method=flickr.photos.search&format=json&api_key=API_KEY&text=TEXT';

  constructor(private http: HttpClient) {}

  searchPhotos(searchTerm: any) {
    console.log('Entering searchPhotos, search for: ' + searchTerm);
    const url = this.urlBase
      .replace('API_KEY', this.key)
      .replace('TEXT', searchTerm);
    console.log('url: ' + url);
    return this.http.get(url, {responseType: 'text'});
  }
}