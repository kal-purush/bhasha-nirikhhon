/**
 * Created by natha on 23.01.2017.
 */
import {Component, OnInit, NgZone} from '@angular/core';
import {ipcRenderer} from 'electron';
import {Media} from "../../shared/video/Media";
import {Mediatype} from "../../shared/enum/mediaType";

@Component({
    selector: 'custom-media-picker',
    templateUrl: 'app/components/mediaPicker/mediaPicker.component.html'
})

export class MediaPickerComponent {
    medias:Media;
    mediaType = Mediatype;

    constructor() {

    }

    openFileUpload():void {
        this.fileUpload(null, "test");
    }

    fileUpload(e, src?:String):void {
        var files:File = e.dataTransfer.files;
        Object.keys(files).forEach((key) => {
            console.log(files[key]);
        })
    }
}