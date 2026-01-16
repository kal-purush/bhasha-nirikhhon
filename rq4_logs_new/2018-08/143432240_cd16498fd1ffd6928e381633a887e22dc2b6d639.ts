import {Directive, ElementRef, Renderer2, HostListener} from '@angular/core';

@Directive({
  selector: '[appMyValid]'
})
export class ValidateDirective {

  // constructor(private elementRef: ElementRef) {
  //   this.elementRef.nativeElement.style.color = 'green';
  // }

  constructor(private element: ElementRef, private renderer: Renderer2) {
    this.renderer.setStyle(this.element.nativeElement, 'cursor', 'pointer');
  }

  @HostListener('mouseenter') onMouseEnter() {
    this.setFontWeight('green');
  }

  @HostListener('mouseleave') onMouseLeave() {
    this.setFontWeight('red');
  }

  private setFontWeight(val: string) {
    this.renderer.setStyle(this.element.nativeElement, 'color', val);
  }
}