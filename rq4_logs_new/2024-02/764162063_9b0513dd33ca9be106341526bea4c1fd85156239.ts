import { AfterViewInit, Directive, ElementRef, Input, OnInit, Renderer2 } from '@angular/core';

@Directive({
  selector: '[truncate]',
  standalone: true
})

export class TruncateDirective implements AfterViewInit {
  @Input('truncate') maxLength: number = 50;

  constructor(private el: ElementRef, private renderer: Renderer2) {}

  ngAfterViewInit() {
    this.truncateText();
  }

  private truncateText() {
    const text = this.el.nativeElement.innerText;
    console.log(this.maxLength, text.length);
    if (text.length > this.maxLength) {
      const truncatedText = text.substring(0, this.maxLength) + '...';
      this.renderer.setProperty(this.el.nativeElement, 'innerText', truncatedText);
    }
  }
}