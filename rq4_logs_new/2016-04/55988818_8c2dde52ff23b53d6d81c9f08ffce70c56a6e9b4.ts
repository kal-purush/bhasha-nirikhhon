/**
 * Created by scott on 4/11/16.
 */

///<reference path="deck.ts" />

module Player {
    export class Player {

        private _name:string;
        private _playPile = null;
        private _wonPile = null;


        constructor(n:string) {
            this._name = n;
            this._playPile = new Deck.Pile();
            this._wonPile = new Deck.Pile();
        }

        public playCard() {
            if(this._playPile.pileSize() === 0){
                this.useWonPile()
            }
            if(this._playPile.pileSize()> 0){
                return this._playPile.getTopCard();
            }
            return null;
          }

        public getName() {
            return this._name;
        }

        public getNumCards() {
            return this._playPile.pileSize()+this._wonPile.pileSize();
        }

        public useWonPile() {
            this._playPile.addCards(this._wonPile);

        }

        public collectCard(theDeck) {
            this._wonPile.addCard(theDeck)
        }

        public collectCards(thePile) {
            this._wonPile.addCards(thePile)
        }
    }


}