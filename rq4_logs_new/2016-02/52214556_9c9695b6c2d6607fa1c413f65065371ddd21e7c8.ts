import {Component} from 'angular2/core';
import {HighlightDirective} from './attribute-directives/highlight/highlight';
import {DraggableDirective} from './attribute-directives/draggable/draggable.directive';

interface Card{
  id: number;
  title: string;
}

var CARDS: Card[] = [];

@Component({
  selector: 'my-app',
  directives: [HighlightDirective, DraggableDirective],
  template:`
    <div class="tasks">
    <div class="task" myHighlight myDraggable *ngFor="#card of cards" [class.selected]="card === selectedCard" (click)="onSelect(card)">{{card.title}}</div>
    </div>
    `,
    styles:[`
      .tasks {
         display: inline-flex;
      }

      .task {
        width:        250px;
        height:       75px;
        border:       1px solid black;
        margin-right: 10px;
      }

      .task:last-child {
        margin-right: inherit;
      }

      .selected {
        color:      blue;
        background-color: green !important;
      }

      .highlight {
        background-color: yellow;
      }
      `]
})
export class AppComponent {
  public title = 'Tour of Heroes';
  public cards = CARDS;
  private selectedCard: Card;

  onSelect(card: Card) { this.selectedCard = card; }

  constructor() {
    let i;
    for (i = 0; i < 7; i++) {
      CARDS.push({id: i, title: `Task ${i+1}`});
    }

    console.log('cards', CARDS);
  }
}