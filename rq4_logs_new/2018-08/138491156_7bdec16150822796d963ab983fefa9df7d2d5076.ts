import { Directive, ElementRef, Input, Renderer2, SimpleChanges } from '@angular/core';

@Directive({
  selector: '[background-image]'
})
export class BackgroundImage {

  private el: HTMLElement;
  @Input('background-image') backgroundImage: string;

  constructor(private renderer: Renderer2, private elRef: ElementRef) {
    this.el = this.elRef.nativeElement;
  }

  ngAfterViewInit() {
    this.setBackgroundImage();
  }

  ngOnChanges(changes: SimpleChanges) {
    if ( changes['backgroundImage'] )
      this.setBackgroundImage();
  }

  setBackgroundImage(){
    this.renderer.setStyle(this.el, "backgroundImage", `url(${ this.backgroundImage })`);
  }
}