import { Component } from '@angular/core';

@Component({
  selector: 'app-popover-content',
  templateUrl: './popover-content.component.html',
})
export class PopoverContentComponent {
  public loaded = false;

  constructor() {
    setTimeout(() => {
      this.loaded = true
    }, 1000);
  }

}