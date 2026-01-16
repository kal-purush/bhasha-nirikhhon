import {Injectable} from '@angular/core';
import {LooperAudioContext} from "../lib/LooperAudioContext";

@Injectable()
export class AnalyzeService {
    private _audioctx: AudioContext;
    private _analyze: AnalyserNode;
    private _analyzeMode: number;
    private _timerId: number;

    constructor() {
        this._audioctx    = LooperAudioContext.getInstance();
        this._analyze     = this._audioctx.createAnalyser();
        this._analyzeMode = 0;
    }

    get timerId(): any {
        return this._timerId;
    }

    set timerId(value: any) {
        this._timerId = value;
    }

    connect(source: MediaStreamAudioSourceNode) {
        source.connect(this._analyze);
    }

    disconnect(source: MediaStreamAudioSourceNode) {
        source.disconnect(this._analyze);
        cancelAnimationFrame(this._timerId);
    }

    analyze(canvas: any) {
        const ctx = canvas.getContext("2d");

        this._analyze.fftSize = 1024;

        const DrawGraph = () => {
            ctx.fillStyle = "rgba(34, 34, 34, 1.0)";
            ctx.fillRect(0, 0, 512, 256);
            ctx.strokeStyle = "rgba(255, 255, 255, 1)";
            const data      = new Uint8Array(512);

            if (this._analyzeMode === 0) {
                this._analyze.getByteFrequencyData(data);
            } else {
                this._analyze.getByteTimeDomainData(data);
            }

            if (this._analyzeMode !== 0) {
                ctx.beginPath();
            }

            for (let i = 0; i < 256; ++i) {
                if (this._analyzeMode === 0) {
                    ctx.fillStyle = "rgba(204, 204, 204, 0.8)";
                    ctx.fillRect(i * 2, 256 - data[i], 1, data[i]);
                } else {
                    ctx.lineTo(i * 2, 256 - data[i]);
                }
            }
            if (this._analyzeMode !== 0) {
                ctx.stroke();
            }
            requestAnimationFrame(DrawGraph);
        };

        this._timerId = requestAnimationFrame(DrawGraph);
    }
}