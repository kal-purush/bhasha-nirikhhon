/*import { HttpClient } from '@angular/common/http';
import { Injectable } from '@angular/core';


  Generated class for the YtProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.

@Injectable()
export class YtProvider {

  constructor(public http: HttpClient) {
    console.log('Hello YtProvider Provider');
  }

}*/


import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';
 
@Injectable()
export class YtProvider {
  apiKey = 'AIzaSyA8xzAqh0n_rtGAb9Y-q2cmSi_eK0kPba8';
 
  constructor(public http: Http) { }
 
  getPlaylistsForChannel(channel) {
    return this.http.get('https://www.googleapis.com/youtube/v3/playlists?key=' + this.apiKey + '&channelId=' + channel + '&part=snippet,id&maxResults=20')
    .map((res) => {
      return res.json()['items'];
    })
  }
 
  getListVideos(listId) {
    return this.http.get('https://www.googleapis.com/youtube/v3/playlistItems?key=' + this.apiKey + '&playlistId=' + listId +'&part=snippet,id&maxResults=20')
    .map((res) => {
      return res.json()['items'];
    })
  }
}