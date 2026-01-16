import {Component, OnInit, Input, ElementRef} from 'angular2/core';
import * as AppConstants from '../../app/app.constants';

declare var jQuery: any;

@Component({
  selector: 'mdb-backdrop',
  template: `
    <div class="backdrop-container fader" [style.opacity]="loaded ? '1.0' : '0'">
      <img class="backdrop-image" src="{{API_SETTINGS.baseImageUrl}}w1920/{{path}}">
    </div>
  `
})

export class BackdropComponent {

  @Input() path: string;
  API_SETTINGS: Object = AppConstants.API_SETTINGS;
  loaded: Boolean;

  constructor(private _elementRef:ElementRef){
    var img = jQuery(this._elementRef.nativeElement).find('.backdrop-image');

    img.on('load', () => {
      this.loaded = true;
    });
  }
}