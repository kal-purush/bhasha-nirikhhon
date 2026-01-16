import { Directive, ElementRef, Renderer2, OnInit, Input } from '@angular/core';
import { AbstractControl } from '@angular/forms';

@Directive({ selector: '[control-form-error]' })
export class ControlFormErrorDirective implements OnInit {
  @Input() control: AbstractControl | undefined;

  constructor(private elRef: ElementRef, private renderer: Renderer2) {}

  ngOnInit(): void {
    this.control?.valueChanges.subscribe(() => {
      console.log('value change');
      if (this.control && this.control.dirty && !this.control.valid) {
        this.renderer.addClass(this.elRef.nativeElement, 'control-form-error');
      } else if (this.control?.valid) {
        this.renderer.removeClass(this.elRef.nativeElement, 'control-form-error');
      }
    });
  }
}