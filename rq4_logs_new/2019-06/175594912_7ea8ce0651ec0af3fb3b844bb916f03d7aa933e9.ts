import { ElementRef } from '@angular/core';

export class ChatScroll {
  disableScrollDown = false;
  scrollNative: any;
  atBottom: boolean;

  constructor(private scroller: ElementRef) {}

  scrollToBottom(): void {
    if (!this.disableScrollDown) {
      try {
        this.scroller.nativeElement.scrollTop = this.scroller.nativeElement.scrollHeight;
      } catch (err) {}
    }
  }

  reset() {
    this.disableScrollDown = false;
    this.scrollToBottom();
  }

  manageAutoScroll() {
    this.setScrollState();
    this.disableScrollIfInterrupted();
  }

  setScrollState() {
    this.scrollNative = this.scroller.nativeElement;
    this.atBottom = this.scrollNative.scrollHeight - this.scrollNative.scrollTop === this.scrollNative.clientHeight;
  }

  disableScrollIfInterrupted() {
    if (this.atBottom) {
      this.disableScrollDown = false;
    } else {
      this.disableScrollDown = true;
    }
  }
}