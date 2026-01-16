import { Directive, Renderer2, ElementRef, AfterViewInit } from '@angular/core';
import { PaymentStatus } from '@gmrc/enums';

@Directive({
  selector: '[appPaymentStatus]'
})
export class PaymentStatusDirective implements AfterViewInit {

  constructor(
    private renderer: Renderer2,
    private elRef: ElementRef
  ) { }
  ngAfterViewInit() {
    const paymentStatus = this.elRef.nativeElement.textContent;
    if (paymentStatus === PaymentStatus.BALANCE) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'orange');
    } else if (paymentStatus === PaymentStatus.UNPAID) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'red');
    } else if (paymentStatus === PaymentStatus.PAID) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'green');
    }
  }
}