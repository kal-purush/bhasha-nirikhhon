import { Injectable } from '@angular/core';

import { ScreenOrientation } from '@ionic-native/screen-orientation/ngx';


@Injectable({
    providedIn: 'root',
})
export class DataService {
    constructor(private screenOrientation: ScreenOrientation) {
        this.screenOrientation.lock(this.screenOrientation.ORIENTATIONS.PORTRAIT);
     }
}