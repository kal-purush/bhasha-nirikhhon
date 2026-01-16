import {Component, Input} from 'angular2/core';

@Component({
  selector: 'zippy',
  template: `
  <div class="zippy">
    <div
      class="zippy-title"
      (click)="toggle()">
      {{ title }}
      <i
        class="pull-right glyphicon"
        [ngClass]="{
          'glyphicon-chevron-down': !isExpanded,
          'glyphicon-chevron-up': isExpanded
        }">
      </i>
    </div>
    <div *ngIf="isExpanded" class="zippy-content">
      <ng-content></ng-content>
    </div>
  </div>
  `
})

export class ZippyComponent {
  isExpanded = false;
  // input property so that we can set it from the outside
  @Input() title: string;

  toggle() {
    this.isExpanded = !this.isExpanded;
  }
}