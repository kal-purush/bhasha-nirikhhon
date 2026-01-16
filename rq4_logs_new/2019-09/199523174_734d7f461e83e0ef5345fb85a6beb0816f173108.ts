import { Component, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { fromEvent } from 'rxjs';
import { mergeMap, takeUntil, map } from 'rxjs/operators';
import { PaintService } from './paint.service';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.css']
})
export class AppComponent implements AfterViewInit {
  x;
  y;
  constructor(private paintService: PaintService) {}

  @ViewChild('canvas', {static: false}) canvas: ElementRef;

  ngAfterViewInit(){
    this.paintService.init(this.canvas.nativeElement);
    this.start();
  }

  private start() {

    const { nativeElement } = this.canvas;
    const mousedown$ = fromEvent(nativeElement, 'mousedown');
    const mouseup$ = fromEvent(nativeElement, 'mouseup');
    const mousemove$ = fromEvent(nativeElement, 'mousemove');

    const paint = mousedown$
      .pipe(
        mergeMap(() => {
          return mousemove$.pipe(
            map((e: any) => ({
              x: e.clientX,
              y: e.clientY
            })),
            takeUntil(mouseup$)
          );
        })
      );

    const offset = getOffset(nativeElement);

    paint.subscribe((event) => {
      const x = event.x - offset.left;
      const y = event.y - offset.top;
      this.paintService.paint({x, y});
    });
  }
}
function getOffset(el: HTMLElement) {
  const rect = el.getBoundingClientRect();

  return {
    top: rect.top + document.body.scrollTop,
    left: rect.left + document.body.scrollLeft
  };
}