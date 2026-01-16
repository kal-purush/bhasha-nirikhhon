import { Injectable } from '@angular/core';
import { Subject } from 'rxjs';

@Injectable()
export class ScreenService {
  width: number;
  height: number;
  resizeChanges = new Subject<void>();

  constructor() {
    window.onresize = () => {
      this.resize();
      this.resizeChanges.next();
    };
    this.resize();
  }

  resize(): void {
    if (window.innerWidth) {
      this.width = window.innerWidth;
      this.height = window.innerHeight;
    } else if (
      document.documentElement &&
      document.documentElement.clientWidth
    ) {
      this.width = document.documentElement.clientWidth;
      this.height = document.documentElement.clientHeight;
    } else {
      this.width = document.body.clientWidth;
      this.height = document.body.clientHeight;
    }
  }
}