import { Component, OnInit, Input, Output, EventEmitter, SimpleChanges } from '@angular/core';

import { MagicApiService } from '../magic-api/magic-api.service';
import { MagicCard } from '../magic-card/magic-card';
import { MagicCardParserService } from '../magic-card-parser/magic-card-parser.service';

@Component({
  selector: 'app-card-search',
  templateUrl: './card-search.component.html',
  styleUrls: ['./card-search.component.css'],
  providers: [MagicApiService, MagicCardParserService]
})
export class CardSearchComponent implements OnInit {

  cardName: string;

  @Output()
  cardsAquired: EventEmitter<MagicCard[]> = new EventEmitter<MagicCard[]>();

  cards: MagicCard[];

  constructor(private magicApiService: MagicApiService,
              private magicCardParserService: MagicCardParserService) { }

  getObservable() {
    return this.magicApiService.getCards({
      params : { name : `${this.cardName}` }
    });
  }

  getData() {
    this.getObservable().subscribe(data => {

      this.cards = this.magicCardParserService.parseCards(data);
      console.log(this.cards);

      // If the cardName is exactly the name of an existing card then only
      // put this card in the cards list
      if (this.cards.some(x => x.name === this.cardName)) {
        const newCards: MagicCard[] = [];
        for (const card of this.cards) {
          if (card.name === this.cardName) {
            newCards.push(card);
          }
        }
        this.cards = newCards;
      }

      this.cardsAquired.emit(this.cards);

      } );
  }

  updateName(event) {
    this.cardName = event;
    this.getData();
  }

  ngOnInit() {
  }

}