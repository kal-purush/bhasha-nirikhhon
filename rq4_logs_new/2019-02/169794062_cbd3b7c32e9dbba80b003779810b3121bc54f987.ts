import { SpotifyService } from './../spotify/spotify.service';
import { Injectable } from '@angular/core';
import { promise } from 'protractor';

@Injectable({
    providedIn: 'root'
})
export class MosaicService {

    constructor(
        public spotifyService: SpotifyService
    ) { }

    getMosaicImages() {
        return new Promise((resolve, reject) => {
            let mosaic_images: Array<any> = [];
            this.spotifyService.get('user_top', { type: 'tracks' }, { limit: 50 }).subscribe(
                response => {
                    const x = Number(( ( window['innerHeight'] / ( window['innerWidth'] * 0.1 ) ) / 5 ).toString().split('.')[0]) + 1;
                    for (let index = 0; index < x; index++) {
                        mosaic_images = this.shuffle(mosaic_images.concat(response.items.map(item => item.album.images[0].url)));
                    }
                    resolve(mosaic_images);
                },
                error => {
                    resolve(error);
                }
            );
        })
    }

    shuffle(array: Array<any>) {
        let currentIndex = array.length, temporaryValue, randomIndex;
        while (0 !== currentIndex) {
            randomIndex = Math.floor(Math.random() * currentIndex);
            currentIndex -= 1;
            temporaryValue = array[currentIndex];
            array[currentIndex] = array[randomIndex];
            array[randomIndex] = temporaryValue;
        }
        return array;
    }

    getMosaicSize() {
        return (window.innerWidth / 10) + 'px';
    }
}