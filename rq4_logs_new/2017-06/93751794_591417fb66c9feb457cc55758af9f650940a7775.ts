import { Directive, ElementRef, Renderer } from '@angular/core';

@Directive({
  selector: '[appAutoGrow]',
  host: {
      '(focus)': 'onFocus()',
      '(blur)': 'onBlur()'
  }
})
export class AutoGrowDirective {
  constructor(
   private el: ElementRef,
   private renderer: Renderer
  ) { }
  onFocus() {
      console.log("inside");
   this.renderer.setElementStyle(this.el.nativeElement, 'width', '200');
  }
  onBlur() {
      console.log("outside");
   this.renderer.setElementStyle(this.el.nativeElement, 'width', '100');
  }
}