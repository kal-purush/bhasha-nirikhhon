import {Injectable} from '@angular/core';
import {LooperAudioContext} from "../lib/LooperAudioContext";

@Injectable()
export class DestinationService {
    private _audioctx: AudioContext;
    private _isConnect: boolean;

    constructor() {
        this._audioctx  = LooperAudioContext.getInstance();
        this._isConnect = false;
    }

    get isConnect(): boolean {
        return this._isConnect;
    }

    connect(source: MediaStreamAudioSourceNode) {
        source.connect(this._audioctx.destination);
        this._isConnect = true;
    }

    disconnect(source: MediaStreamAudioSourceNode) {
        source.disconnect(this._audioctx.destination);
        this._isConnect = false;
    }
}