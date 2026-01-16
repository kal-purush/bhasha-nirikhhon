import { AfterViewInit, Directive, ElementRef, EventEmitter, HostListener, Input, OnChanges, OnInit, Output, SimpleChanges } from '@angular/core';

@Directive({
  selector: '[restoreScrollPosition]'
})
export class RestoreScrollPositionDirective implements OnChanges, OnInit, AfterViewInit {
  @Input() scrollTopRatio: number = 0;
  @Output() scrollTopRatioChanged: EventEmitter<number> = new EventEmitter<number>();

  constructor(private element: ElementRef) { }

  ngOnChanges(changes: SimpleChanges): void {
    if (changes.scrollPosition) {
      this.ensureScrollPosition(this.scrollTopRatio);
    }
  }

  ngOnInit(): void {
  }

  ngAfterViewInit(): void {
    this.ensureScrollPosition(this.scrollTopRatio);
  }

  ensureScrollPosition(scrollTopRatio: number) {
    scrollTopRatio = scrollTopRatio || 0;
    setTimeout(() => {
      const scrollTop = this.calculateScrollTop(scrollTopRatio);
      if (this.element.nativeElement.scrollTop !== scrollTop) {
        this.element.nativeElement.scrollTop = scrollTop;
      }
    })
  }

  @HostListener('scroll', ['$event'])
  onScroll(event) {
    const ratio = this.calculateScrollTopRatio(event.target.scrollTop);
    if (this.scrollTopRatio !== ratio) {
      this.scrollTopRatio = ratio;
      this.scrollTopRatioChanged.emit(this.scrollTopRatio);
    }
  }

  private calculateScrollTop(scrollTopRatio: number): number {
    return scrollTopRatio * (this.element.nativeElement.scrollHeight - this.element.nativeElement.offsetHeight);
  }

  private calculateScrollTopRatio(scrollTop: number): number {
    return scrollTop / (this.element.nativeElement.scrollHeight - this.element.nativeElement.offsetHeight);
  }
}