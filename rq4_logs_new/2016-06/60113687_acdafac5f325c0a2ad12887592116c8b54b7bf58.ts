import {Directive} from '@angular/core';
import {ElementRef} from '@angular/core';
import {OnInit} from '@angular/core';
import {Renderer} from '@angular/core';

@Directive({
  selector: '[myHighLight]',
  inputs: ['highLightColor:myHighLight'],
  host: {
    '(mouseenter)': 'onMouseEnter()',
    '(mouseleave)': 'onMouseLeave()'    
  }

})
export class HighLightDirective{  
  private _defaultColor = 'green';
  highLightColor: string;

  constructor(private _elRef: ElementRef, private _renderer: Renderer) {}

onMouseEnter(){
  this.highlight(this.highLightColor || this._defaultColor);
}
onMouseLeave(){
  this.highlight(null);
}
private highlight(color: string) {
  this._renderer.setElementStyle(this._elRef, 'background-color', color);

}

}