/**
 * Created by sangress on 1/5/16.
 */

import {Component, Input} from 'angular2/core';
import {NgFor} from 'angular2/common';

@Component({
  selector: 'img-displayer',
    template: `
    <div class="media" *ngFor="#image of images">
      <div class="media-left">
        <a href="#">
          <img class="media-object" src="{{ image.path }}" style="max-width:200px">
        </a>
      </div>
      <div class="media-body">
        <h4 class="media-heading">{{ image.name }}</h4>
        <p>{{ image.size }} bytes</p>
      </div>
    </div>
    `
})

export class ImgDisplayer {
    //@Input() images: Array<Object>;
    @Input() images: Object[];

    constructor (){}


}