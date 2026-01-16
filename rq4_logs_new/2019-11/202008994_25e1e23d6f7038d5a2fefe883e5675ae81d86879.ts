import { Directive, ElementRef, Renderer2, OnInit, AfterViewInit } from '@angular/core';
import { PaymentStatus } from '@gmrc/enums';

@Directive({
  selector: '[appRoomPaymentStatus]'
})
export class RoomPaymentStatusDirective implements OnInit, AfterViewInit {

  constructor(private renderer: Renderer2, private elRef: ElementRef) { }

  ngOnInit() {

  }

  ngAfterViewInit() {
    const paymentStatus = this.elRef.nativeElement.textContent;
    console.log(paymentStatus);
    if (paymentStatus === PaymentStatus.BALANCE) {
      console.log(paymentStatus);
      this.renderer.setStyle(this.elRef.nativeElement, 'background', 'red');
    }
  }

}