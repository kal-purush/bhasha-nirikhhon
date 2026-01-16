import { Component, View, NgClass, Output, EventEmitter, FORM_DIRECTIVES } from 'angular2/angular2';

@Component({
  selector: 'search-box'
})
@View({
  templateUrl: 'src/shared/search-box.html',
  styleUrls: ['src/shared/search-box.css'],
  directives: [FORM_DIRECTIVES]
})
export class SearchBox {

  @Output() changed: EventEmitter = new EventEmitter();

  public value: string;
  constructor() {

  }

  public clear() {
    this.value = "";
    this.changed.next(this.value);
  }

  private onKey(event: any): void
  {
    this.changed.next(this.value);
  }

}