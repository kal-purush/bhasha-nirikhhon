import { Directive, ElementRef, Renderer2, OnInit, AfterViewInit } from '@angular/core';
import { PaymentStatus } from '@gmrc/enums';

@Directive({
  selector: '[appRoomPaymentStatus]'
})
export class RoomPaymentStatusDirective implements OnInit, AfterViewInit {

  constructor(
    private renderer: Renderer2,
    private elRef: ElementRef
    ) { }

  ngOnInit() {
  }
  ngAfterViewInit() {
    const paymentStatus = this.elRef.nativeElement.textContent;
    if (paymentStatus === PaymentStatus.BALANCE) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'orange');
      this.renderer.setStyle(this.elRef.nativeElement, 'height', '33px');
      this.renderer.setStyle(this.elRef.nativeElement, 'margin-bottom', '0px');
      this.renderer.setStyle(this.elRef.nativeElement, 'text-align', 'center');
      this.renderer.setStyle(this.elRef.nativeElement, 'padding-bottom', '10px');
    } else if (paymentStatus === PaymentStatus.UNPAID) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'red');
      this.renderer.setStyle(this.elRef.nativeElement, 'height', '50px');
      this.renderer.setStyle(this.elRef.nativeElement, 'border-bottom', '4px solid rgba(0, 0, 0, 0.12)');
      this.renderer.setStyle(this.elRef.nativeElement, 'padding-bottom', '10px');
      this.renderer.setStyle(this.elRef.nativeElement, 'display', 'flex');
      this.renderer.setStyle(this.elRef.nativeElement, 'align-items', 'center');
      this.renderer.setStyle(this.elRef.nativeElement, 'justify-content', 'center');
    } else if (paymentStatus === PaymentStatus.PAID) {
      this.renderer.setStyle(this.elRef.nativeElement, 'color', 'green');
      this.renderer.setStyle(this.elRef.nativeElement, 'height', '50px');
      this.renderer.setStyle(this.elRef.nativeElement, 'border-bottom', '4px solid rgba(0, 0, 0, 0.12)');
      this.renderer.setStyle(this.elRef.nativeElement, 'padding-bottom', '10px');
      this.renderer.setStyle(this.elRef.nativeElement, 'display', 'flex');
      this.renderer.setStyle(this.elRef.nativeElement, 'align-items', 'center');
      this.renderer.setStyle(this.elRef.nativeElement, 'justify-content', 'center');
    } else if (paymentStatus === PaymentStatus.NONE) {
      this.renderer.setStyle(this.elRef.nativeElement, 'height', '50px');
      this.renderer.setStyle(this.elRef.nativeElement, 'border-bottom', '4px solid rgba(0, 0, 0, 0.12)');
      this.renderer.setStyle(this.elRef.nativeElement, 'padding-bottom', '10px');
      this.renderer.setStyle(this.elRef.nativeElement, 'display', 'flex');
      this.renderer.setStyle(this.elRef.nativeElement, 'align-items', 'center');
      this.renderer.setStyle(this.elRef.nativeElement, 'justify-content', 'center');
    }
  }

}