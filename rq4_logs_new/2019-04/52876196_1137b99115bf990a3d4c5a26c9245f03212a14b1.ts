import { Directive, Output, EventEmitter, AfterViewInit, OnDestroy, Inject, ElementRef } from '@angular/core';
import { DOCUMENT } from '@angular/common';
import { fromEvent, Subscription } from 'rxjs';

@Directive({
  selector: '[nglClickOutside]'
})
export class NglClickOutsideDirective implements AfterViewInit, OnDestroy {

  @Output('nglClickOutside') clickOutside: EventEmitter<void> = new EventEmitter();

  private subscription: Subscription;

  constructor(@Inject(DOCUMENT) private document: any, private element: ElementRef) {}

  ngAfterViewInit() {
    this.subscription = fromEvent(this.document, 'mousedown').subscribe((e: MouseEvent) => {
      if (this.shouldClose(e)) {
        this.clickOutside.emit();
      }
    });
  }

  ngOnDestroy() {
    if (this.subscription) {
      this.subscription.unsubscribe();
      this.subscription = null;
    }
  }

  private shouldClose(event: MouseEvent | TouchEvent) {
    const element = event.target as HTMLElement;
    if ((event instanceof MouseEvent && event.button === 2)) {
      return false;
    }
    return !this.element.nativeElement.contains(element);
  }
}