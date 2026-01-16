import {Directive, HostBinding, HostListener} from '@angular/core';

@Directive({
  selector: '[appBasicHighlightHostbinding]'
})

export class BasicHighlightHostbindingDirective {
  @HostBinding('style.backgroundColor') colorOfBack: string = 'blue';

  constructor() {
  }

  @HostListener('mouseenter') onMouseOver() {
    this.colorOfBack = 'orange';
  }

  @HostListener('mouseleave') onMouseLeave() {
    this.colorOfBack = 'blue';
  }
}