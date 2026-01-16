import { Component, AfterViewInit, ViewChild, ElementRef } from '@angular/core';

import { LatLong } from '../lat-long';
import { DataService } from '../data.service';
import { LocationService, LocationStatusData } from '../location.service';
import { Utilities } from '../utilities';

@Component({
  selector: 'app-plot-map',
  templateUrl: './plot-map.component.html',
  styleUrls: ['./plot-map.component.css']
})
export class PlotMapComponent implements AfterViewInit {

  targets: Array<LatLong & { distance: number }>;
  private status: LocationStatusData;

  @ViewChild('canvas') private canvasElementRef: ElementRef;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D;

  constructor(
    private dataService: DataService,
    private locationService: LocationService
  ) { }

  ngAfterViewInit() {
    const dpr = window.devicePixelRatio;

    this.canvas = this.canvasElementRef.nativeElement;
    this.ctx = this.canvas.getContext('2d');

    const clientWidth = this.canvas.clientWidth;
    const width = clientWidth * dpr;

    this.canvas.width = this.canvas.height = width;
    this.canvas.style.width = this.canvas.style.height = `${clientWidth}px`;

    this.getData();
  }

  getData() {
    this.locationService.getLocationStatus().subscribe(s => {
      this.status = s;
      this.draw();
    });

    this.dataService.getTargets().subscribe(t => {
      this.targets = t.map(target => {
        return { ...target, distance: Utilities.getDistance(target, this.dataService.currentBase) };
      });

      this.draw();
    });
  }


  private draw() {
    /**
     * TODO
     * if - no base - return
     * else if - base, no targets, no current - return
     * else - base, (targets || current) - draw (figure out max distances)
     */
    if (!this.targets || this.targets.length === 0) {
      return;
    }

    const dpr = window.devicePixelRatio;
    /** width of the canvas */
    const w = this.canvas.width;
    /** size of square that contains the icons and font */
    const iconSize = 25 * dpr;
    /** padding of the ... */
    const p = iconSize / 2 + (1 * dpr /* extra breathing space */);
    /** vertical centerline of the canvas */
    const c = this.canvas.height / 2;
    /** width used for stroking lines */
    const lineWidth = 5 * dpr;

    const base = this.dataService.currentBase;
    const currentLocation = this.status.position ? this.status.position.coords : null;

    const minLat = this.targets.reduce((accumulator, currentTarget) =>
      Math.min(accumulator, currentTarget.latitude),
      Math.min(currentLocation ? currentLocation.latitude : Number.MAX_SAFE_INTEGER, base.latitude));
    const minLon = this.targets.reduce((accumulator, currentTarget) =>
      Math.min(accumulator, currentTarget.longitude),
      Math.min(currentLocation ? currentLocation.longitude : Number.MAX_SAFE_INTEGER, base.longitude));
    const maxLat = this.targets.reduce((accumulator, currentTarget) =>
      Math.max(accumulator, currentTarget.latitude),
      Math.max(currentLocation ? currentLocation.latitude : Number.MIN_SAFE_INTEGER, base.latitude));
    const maxLon = this.targets.reduce((accumulator, currentTarget) =>
      Math.max(accumulator, currentTarget.longitude),
      Math.max(currentLocation ? currentLocation.longitude : Number.MIN_SAFE_INTEGER, base.longitude));
    const latDiff = maxLat - minLat;
    const lonDiff = maxLon - minLon;
    const isLatDiffBigger = latDiff > lonDiff;
    const ratio = (w - iconSize) / (isLatDiffBigger ? latDiff : lonDiff);
    const leftPadding = (w - iconSize / 2 - lonDiff * ratio) / 2;

    console.log(`
minLat: ${minLat}
maxLat: ${maxLat}
latDiff: ${latDiff}
minLong: ${minLon}
maxLon: ${maxLon}
lonDiff: ${lonDiff}
isLatDiffBigger: ${isLatDiffBigger}
canvas side: ${w}
iconSize: ${iconSize}
ratio: ${ratio}
`);

    this.ctx.clearRect(0, 0, this.canvas.width, this.canvas.height);

    // TODO My markers appear to leave padding on the bottom and right sides of the canvas.  Why?

    // TODO Add option to set the origin at the current location

    // TODO Add concentric circles with distance text.
    // Orign of circle should be base or current, based on some option.

    // Draw base marker
    Utilities.drawMarker(
      this.ctx,
      iconSize,
      '#00DB4A',
      leftPadding + (base.longitude - minLon) * ratio,
      w - (base.latitude - minLat) * ratio
    );

    // Draw current location marker
    if (currentLocation) {
      Utilities.drawMarker(
        this.ctx,
        iconSize,
        '#09c',
        leftPadding + (currentLocation.longitude - minLon) * ratio,
        w - (currentLocation.latitude - minLat) * ratio
      );
    }

    // Draw target markers
    this.targets.forEach(t =>
      Utilities.drawMarker(
        this.ctx,
        iconSize,
        '#fff',
        leftPadding + (t.longitude - minLon) * ratio,
        w - (t.latitude - minLat) * ratio
      )
    );


  }
}