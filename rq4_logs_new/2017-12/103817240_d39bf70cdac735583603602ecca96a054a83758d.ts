import {Component, EventEmitter, Input, Output} from '@angular/core';
import {Team} from "../../model/Team";

/**
 * Generated class for the FilterViewComponent component.
 *
 * See https://angular.io/api/core/Component for more info on Angular
 * Components.
 */
@Component({
  selector: 'filter-view',
  templateUrl: 'filter-view.html'
})
export class FilterViewComponent {
  @Input() teams: Team[] = [];
  @Input() activeTeam: Team;
  @Input() teamCompareFn: Function;
  @Input() start: string;
  @Input() end: string;

  @Output() save = new EventEmitter<any>();
  @Output() cancel = new EventEmitter<void>();

  selectOpts = {title: 'Team'};

  constructor() {
  }

  onSave() {
    const data = {
      activeTeam: this.activeTeam,
      start: this.start,
      end: this.end
    };
    this.save.emit(data);
  }

  onCancel() {
    this.cancel.emit();
  }
}