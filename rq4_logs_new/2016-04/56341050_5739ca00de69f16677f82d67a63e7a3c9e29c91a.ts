import {Component, Output, EventEmitter} from 'angular2/core';
import {RouterLink} from 'angular2/router';

@Component({
  selector: 'action-bar',
  template: require('./action-bar.html'),
  styles: [require('./action-bar.scss')],
  directives: [RouterLink]
})

export class ActionBarCmp {

  @Output() localeChange = new EventEmitter();

  onChange(event) {
    this.localeChange.emit({ locale: event.target.value })
  }
}