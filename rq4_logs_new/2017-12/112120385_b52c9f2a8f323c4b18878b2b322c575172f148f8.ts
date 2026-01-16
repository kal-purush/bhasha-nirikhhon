import { Component, OnInit, ViewChild, ElementRef, AfterViewInit } from '@angular/core';
import { ChangeDetectionStrategy } from '@angular/core';
import { Observable } from 'rxjs/Observable';
import { IPixelCanvas } from '../pixel-canvas.store';
import { NgRedux } from '@angular-redux/store';
import { IAppState } from '../../store';
import { CanvasActions } from '../pixel-canvas.actions';
import { Subscription } from 'rxjs/Subscription';

@Component({
  selector: 'app-image-display',
  templateUrl: './image-display.html',
  styles: [],
  changeDetection: ChangeDetectionStrategy.OnPush
})
export class ImageDisplayComponent implements AfterViewInit {

  @ViewChild('displayCanvas') canvas: ElementRef;
  private context: CanvasRenderingContext2D;
  private subscription: Subscription;
  private amplification = 5;

  canvasData: Observable<IPixelCanvas>;
  
  constructor(private ngRedux: NgRedux<IAppState>, private actions: CanvasActions) {
    // Empty for now, probably going to use later
    this.canvasData = ngRedux.select<IPixelCanvas>(['canvas', 'present']);
  }

  ngAfterViewInit() {
    const canvasElement = <HTMLCanvasElement> this.canvas.nativeElement;
    let context = canvasElement.getContext('2d');
    if (context === null) {
      throw 'Unable to obtain 2D context for canvas.';
    } else {
      this.context = context;
    }

    this.subscription = this.canvasData.subscribe(this.drawCanvas.bind(this));
  }

  ngOnDestroy() {
    this.subscription.unsubscribe();
  }

  private drawCanvas(canvas: IPixelCanvas) {
    this.canvas.nativeElement.width = canvas.width * this.amplification;
    this.canvas.nativeElement.height = canvas.height * this.amplification;
    canvas.pixels.forEach((column: string[], xIndex: number) => {
      column.forEach((pixel: string, yIndex: number) => {
        this.context.fillStyle = pixel;
        this.context.fillRect(xIndex * this.amplification, yIndex * this.amplification,
          this.amplification, this.amplification);
      });
    });
  }

}