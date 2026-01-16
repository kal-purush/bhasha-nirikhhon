import {
  Component,
  Input,
  Output,
  EventEmitter,
  ChangeDetectionStrategy,
} from '@angular/core';

import { GifListItem } from '../gif-list-item';

@Component({
  selector: 'gif-list',
  directives: [GifListItem],
  template: `
    <div>
      <gif-list-item *ngFor="let gif of list" [gif]="gif"
        class="inline-block" (onClick)="onClick.emit($event)">
      </gif-list-item>
      <div *ngIf="list.size === 0">
        No results
      </div>
    </div>
  `,
  changeDetection: ChangeDetectionStrategy.OnPush,
})
export class GifList {
  @Input() list: any;
  @Output() onClick: EventEmitter<number> = new EventEmitter<number>();
};