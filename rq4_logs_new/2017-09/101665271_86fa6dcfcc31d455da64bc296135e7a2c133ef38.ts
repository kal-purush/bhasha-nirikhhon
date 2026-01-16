import { Injectable } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { ConfigService } from '../config/config.service';
import { Http } from '@angular/http';
import * as io from "socket.io-client";
import 'rxjs/Rx';


@Injectable()
export class GroupSocketService {
    //LOCALHOST
    socketHost: string = "http://localhost:8080";
    //IP TO ACCESS WITH ANDROID EMULATOR. COMMENT OUT ALL OTHERS WHEN TESTING WITH ANDROID EMULATOR.
    //socketHost: string = "http://10.0.2.2:8080";
    socket: io;
    uid: Observable<string>;



    constructor(private http: Http) {
        this.uid = this.getUID;
        this.socket = io(this.socketHost);
    }
    private get getUID(): Observable<string> {

        return this.http.get(`${this.socketHost}/group/create`)
        .map(res => res.json().uid )
        .catch((error: any) => Observable.throw(JSON.stringify(error.json())
                || 'Server error'));

    }


}