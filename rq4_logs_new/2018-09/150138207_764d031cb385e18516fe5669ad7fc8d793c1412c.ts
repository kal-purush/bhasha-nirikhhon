import { Directive, ElementRef, Input, HostBinding, OnInit } from '@angular/core';

@Directive({
  selector: '[appLifePoints]'
})




export class LifePointsDirective implements OnInit {

  @Input('appLifePoints') appLifePoints: number;

  constructor(private el: ElementRef) {}

  ngOnInit() {
    const lp = this.appLifePoints || 100;

    this.el.nativeElement.style.width = lp + '%';
    if (lp >= 0 && lp < 30) {
      this.el.nativeElement.style.backgroundColor = '#c70032';
    } else if (lp > 30 && lp < 60) {
      this.el.nativeElement.style.backgroundColor = '#e8a719';
    } else if (lp > 60 && lp <= 100) {
      this.el.nativeElement.style.backgroundColor = '#5fe29a';
    }
  }
}