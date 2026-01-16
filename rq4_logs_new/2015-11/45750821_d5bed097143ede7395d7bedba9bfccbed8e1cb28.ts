"use strict";

import {d} from './d';
import {FFT} from './sound-utils/FFT';
import {Spectrogram} from './sound-utils/Spectrogram';
import {Play} from './sound-utils/Play';
import {SoundLoader} from './sound-utils/SoundLoader';

class Player {
    audioContext = new AudioContext();
    soundLoader = new SoundLoader(this.audioContext);
    play = new Play(this.audioContext);
    spectrogram = new Spectrogram(512);
    canvas:HTMLCanvasElement;
    spectr:HTMLElement;
    audioSelDom:HTMLElement;
    currentTimeDOM:HTMLElement;

    xKoef = 2;
    yKoef = 2;

    constructor() {
        this.defaultLoad();
        this.render();
        this.updateCurrentTime();
    }

    soundLoaded = (audioBuffer:AudioBuffer) => {
        this.play.setAudio(audioBuffer);
        this.drawFFT(audioBuffer);
    };

    drawFFT(audioBuffer:AudioBuffer) {
        this.spectrogram.process(audioBuffer);
        var imd = this.spectrogram.getImageData();
        this.canvas.setAttribute('width', imd.width + '');
        this.canvas.setAttribute('height', imd.height + '');
        this.spectr.style.cssText = `width: ${imd.width / 2 }px; height: ${imd.height / 2 }px`;
        var ctx = this.canvas.getContext('2d');
        ctx.putImageData(imd, 0, 0);
    }

    defaultLoad() {
        this.soundLoader.fromUrl('11.mp3').then(this.soundLoaded);
    }

    getXByTime(time:number) {
        return this.spectrogram.getXByTime(time) / this.xKoef;
    }

    getTimeByX(x:number) {
        return this.spectrogram.getTimeByX(x) * this.xKoef;
    }

    updateCurrentTime() {
        window.requestAnimationFrame(() => {
            this.currentTimeDOM.style.transform = 'translateX(' + this.getXByTime(this.play.getCurrentTime()) + 'px)';
            this.updateCurrentTime();
        });
    }

    render() {
        this.audioSelDom = d('div', {className: 'selection'});
        this.currentTimeDOM = d('div', {className: 'currentTime'});

        this.canvas = <HTMLCanvasElement>d('canvas');
        this.spectr = d('div', {className: 'spectrogram'},
            this.audioSelDom,
            this.canvas,
            this.currentTimeDOM,
            d('div', {className: 'overlay'})
        );

        var audioSelection = new AudioSelection(this);
        d(document.body, null,
            this.spectr,
            d('input', {
                type: 'file',
                onchange: (e:Event)=>this.soundLoader.fromFileInput(<HTMLInputElement>e.target).then(this.soundLoaded)
            }),
            d('div', {className: 'toolbar'},
                d('button', {onclick: ()=>this.play.play(audioSelection.getStartTime(), audioSelection.getDur())}, 'Play'),
                d('button', {onclick: ()=>this.play.stop()}, 'Stop')
            )
        );
    }
}

class AudioSelection {
    isMove = false;
    startX = 0;
    startClientX = 0;
    width = 0;

    constructor(public player:Player) {
        document.addEventListener('mousedown', e => this.start(e));
        document.addEventListener('mousemove', e => this.move(e));
        document.addEventListener('mouseup', e => this.stop());
    }

    start(e:MouseEvent) {
        var node = <Node>e.target;
        var parents:Node[] = [];
        while (node = node.parentNode) {
            parents.push(node);
        }
        if (parents.indexOf(this.player.spectr) > -1) {
            this.isMove = true;
            this.startX = e.offsetX;
            this.startClientX = e.clientX;
            this.width = 0;
            this.render();
            e.preventDefault();
        }
    }

    render() {
        this.player.audioSelDom.style.left = this.getStartX() + 'px';
        this.player.audioSelDom.style.width = this.getWidth() + 'px';
    }

    getStartX() {
        var left = this.startX;
        if (this.width < 0) {
            left += this.width;
        }
        return left;
    }

    getWidth() {
        return Math.abs(this.width);
    }

    getDur() {
        return this.player.getTimeByX(this.getWidth());
    }

    getStartTime() {
        return this.player.getTimeByX(this.getStartX());
    }

    move(e:MouseEvent) {
        if (this.isMove) {
            this.width = e.clientX - this.startClientX;
            this.render();
        }
    }

    stop() {
        this.isMove = false;
    }
}

new Player();

/*

function getLine(fft:Uint8Array[]) {
    var line = new Uint32Array(fft.length);
    for (var i = 0; i < fft.length; i++) {
        var sum = 0;
        for (var j = 0; j < fft[i].length / 2; j++) {
            sum += fft[i][j];
        }
        line[i] = sum;
    }
    return line;
}

function calc(fft:Uint8Array[]) {
    var line = getLine(fft);
    console.time('count');
    var peaksData:number[] = [];
    var width = fft.length;

    for (var step = 100; step < 600; step++) {
        var sum = 0;
        var count = 0;
        for (var i = 0; i < width - step; i += step) {
            for (var p = i + step; p < width - step; p += step) {
                for (var j = 0; j < step; j++) {
                    var diff = line[i + j] - line[p + j];
                    sum += diff > 0 ? diff : -diff;
                    count++;
                }
            }
        }
        peaksData[step] = sum / count | 0;
    }

    var peaks:{diff:number; step: number}[] = [];
    for (var i = 0; i < peaksData.length; i++) {
        var obj = peaksData[i] || Infinity;
        peaks.push({diff: obj, step: i});
    }
    peaks.sort((a, b)=> a.diff < b.diff ? -1 : 1);
    console.log(peaks[0]);
    console.log(peaks);

    console.timeEnd('count');
    return count;
}
*/