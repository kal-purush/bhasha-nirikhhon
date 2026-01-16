import { Directive, ElementRef, HostListener, Renderer } from '@angular/core';

@Directive({
  selector: '[app-change-font-color]'
})
export class ChangeFontColorDirective {

  private color: string;

    constructor(private elementRef: ElementRef, private renderer: Renderer) {
        this.color = 'green';
  }

  @HostListener('mouseover') onItemClick() {
      if (this.elementRef.nativeElement.classList.contains('enlarge')) {
          this.elementRef.nativeElement.classList.remove('enlarge');
          this.renderer.setElementStyle(this.elementRef.nativeElement, 'color', '#028090');
          this.renderer.setElementStyle(this.elementRef.nativeElement, 'font-size', '');
      } else {
          this.elementRef.nativeElement.classList.add('enlarge');
          this.renderer.setElementStyle(this.elementRef.nativeElement, 'color', this.color);
          this.renderer.setElementStyle(this.elementRef.nativeElement, 'font-size', '2.5em');
      }
  }

}