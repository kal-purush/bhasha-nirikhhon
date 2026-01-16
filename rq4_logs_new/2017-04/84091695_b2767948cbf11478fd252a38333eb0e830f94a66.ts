import {Directive, ElementRef, Input, OnInit} from '@angular/core';

const FORTEEN_DAYS: number = 14 * 24 * 60 * 60 * 1000;

@Directive({
  selector: '[trainmeCourseFresh]'
})
export class CourseFreshDirective implements OnInit {

  @Input('trainmeCourseFresh')
  set courseDate(date: string) {
    this._courseDate = new Date(date);
  };

  private _courseDate: Date;

  constructor(private el: ElementRef) {
  }

  ngOnInit(): void {
    console.log(this.courseDate);
    if (this._courseDate < new Date(Date.now()) &&
      this._courseDate >=  new Date(Date.now() - FORTEEN_DAYS)) {
      this.el.nativeElement.style.borderColor = 'green';
    } else {
      this.el.nativeElement.style.borderColor = 'blue';
    }
  }
}