import { Component, AfterContentInit, ContentChild, QueryList, ChangeDetectorRef, OnInit, Input } from '@angular/core';

import { RestListConnectable } from '../mixins/rest-list-connectable';
import { TraitDecorator } from '../util/mixins';

@TraitDecorator(RestListConnectable)
@Component({
  selector: 'cards-list', 
  templateUrl: './cards-list.component.html'
})
export class CardsListComponent implements OnInit {
  @Input() private objects: string;

  @Input('api-url') private apiUrl: string;

  @Input() private source;

  //@ContentChild(CardComponent) private card: CardComponent;

  public ngOnInit() {
    this.connectRest();
  }
}