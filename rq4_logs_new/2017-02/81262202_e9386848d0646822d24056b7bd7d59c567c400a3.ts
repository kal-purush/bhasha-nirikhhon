import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import { Observable } from 'rxjs/Observable';
import 'rxjs/add/operator/map';
import { Album } from '../models/album';

import { Store } from '@ngrx/store';
import * as albumsActions from '../actions/albums';

import { AppStore } from '../models/app-store';

@Injectable()
export class AlbumsService {
    albums: Observable<Array<Album>>;
    private API_PATH: string = 'https://jsonplaceholder.typicode.com/albums';

    constructor(private http: Http, private store: Store<AppStore>) {
        this.albums = store.select('albums');
    }

    getAlbums() {
        this.http.get(this.API_PATH)
            .map(res => res.json())
            .map(payload => ({ type: albumsActions.ActionTypes.LOAD, payload }))
            .subscribe(action => this.store.dispatch(action));
    }

}