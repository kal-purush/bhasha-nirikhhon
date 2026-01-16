import {Injectable} from '@angular/core';
import {HttpClient, HttpHeaders} from '@angular/common/http';
import {AlbumInterface} from '../album-interface';

@Injectable({
    providedIn: 'root'
})
export class AlbumService {
    readonly API_URL = 'http://127.0.0.1:8000/api';

    constructor(private http: HttpClient) {
    }

    createAlbum(data) {
        return this.http.post(`${this.API_URL}/create-album`, data, {headers: this.makeReqHeaderData()});
    }

    getAllAlbum() {
        return this.http.get<AlbumInterface[]>(`${this.API_URL}/get-all-album`, {headers: this.makeReqHeaderJson()});
    }

    showDetailAlbum(id) {
        return this.http.get(`${this.API_URL}/show-album/${id}`, {headers: this.makeReqHeaderJson()});
    }

    makeReqHeaderJson() {
        const token = localStorage.getItem('token');
        return new HttpHeaders({
            'Content-Type': 'application/json',
            Authorization: 'Bearer ' + token
        });
    }

    makeReqHeaderData() {
        const token = localStorage.getItem('token');
        return new HttpHeaders({
            enctype: 'multipart/form-data',
            Authorization: 'Bearer ' + token
        });
    }
}