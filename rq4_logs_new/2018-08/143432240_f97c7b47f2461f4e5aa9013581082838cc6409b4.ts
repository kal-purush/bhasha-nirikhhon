// import {Directive, ElementRef, Renderer2, HostListener} from '@angular/core';
import {Directive, HostListener, Input, TemplateRef, ViewContainerRef, AfterViewInit, Renderer2, ElementRef, ViewChild } from '@angular/core';

@Directive ({
  selector: '[appMyValid]'
})
export class MyValidDirective implements AfterViewInit {
  //
  constructor (private templateRef: TemplateRef<any>, private elementRef: ElementRef, private viewContainer: ViewContainerRef, private _renderer: Renderer2 ) { }
  myTempl = {'context' :  ` <div>  Yaba-daba-doo </div> ` };
  //
  // @ViewChild('appMyValid') _elem: ElementRef;
  ngAfterViewInit() {
    console.log('!!!! NG fuck After bull sheet View mather fucker Init');
    const buttonElement = this._renderer.createElement('button');
    const text = this._renderer.createText('Text');
    console.log(this.elementRef.nativeElement);
    // this._renderer.appendChild(buttonElement, text);
    this._renderer.appendChild(this.elementRef.nativeElement.parentNode, buttonElement);
  }
  @Input() set appMyValid(condition: boolean) {
    if (condition) {
      this.viewContainer.createEmbeddedView(this.templateRef, this.myTempl);
    } else {
      this.viewContainer.clear();
    }
  }
  @HostListener('mouseenter') onMouseEnter() {
    this.setFontWeight('green');
    this.drowSome('sdfgsdhg');
  }
  @HostListener('mouseclick') onMouseClick() {
    this.drowSome('Knock');
  }

  @HostListener('mouseleave') onMouseLeave() {
    this.setFontWeight('blue');
  }
  drowSome(val: string) {
    console.log(val);
  }
  setFontWeight(val: string) {
    console.log(val);
  }
}