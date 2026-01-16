import { Component, OnInit } from '@angular/core';
import { RouteParams } from '@angular/router-deprecated';
import { CardService } from "./card.service";
import { CardTextRow } from "./card-text-row";

@Component({
    selector: "card-detail",
    templateUrl: "app/card-detail.html",
    providers: [CardService]
})

export class CardDetailComponent implements OnInit {
    constructor(
        private cardService: CardService,
        private routeParams: RouteParams
    ) {
        console.log("initialized");
     }
    cardDetails: CardTextRow[] = null;

    ngOnInit() {
        let id = +this.routeParams.get('id');
        this.cardDetails = this.cardService.getCardDetail(id);
    }
    
    goBack() {
    window.history.back();
  }


}