import { Directive, Input, ElementRef, OnChanges, SimpleChanges } from '@angular/core';

@Directive({
  selector: '[setFocus]'
})
export class SetFocusDirective implements OnChanges {

  @Input('setFocus') isFocused: boolean;

  constructor(private hostElement: ElementRef) { }

  ngOnChanges(changes: SimpleChanges) {

    if (changes.isFocused.currentValue) {
      this.hostElement.nativeElement.focus();
    }
  }
}