import { Component, Output, EventEmitter } from '@angular/core';

import { MD_INPUT_DIRECTIVES } from '@angular2-material/input';
import { MD_BUTTON_DIRECTIVES } from '@angular2-material/button';

@Component({
  moduleId: module.id,
  selector: 'app-filters',
  templateUrl: 'filters.component.html',
  styleUrls: ['filters.component.css'],
  directives: [
    MD_BUTTON_DIRECTIVES,
    MD_INPUT_DIRECTIVES,
  ]
})
export class FiltersComponent {
  @Output() filter = new EventEmitter();

  onClick(value) {
    this.filter.emit(value);
  } 
}