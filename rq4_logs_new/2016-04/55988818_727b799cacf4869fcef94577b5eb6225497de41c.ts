/**
 * Created by scott on 4/10/16.
 */

///<reference path="utilities.ts" />

/**
 * Created by scott on 4/10/16.
 */

module Card {
    export class Card {

        private _rank:number;
        private _suit:number;

        constructor(r:number, s:number ) {
            if (r<0 || r>13){
                r=0;
            }
            if (s<0 || s>4){
                s=0;
            }
            this._rank = r;
            this._suit = s;
        }

        public getRank():number {
            if (this._rank === 1) return 14;
            return this._rank;
        }


        public toString() {

            var rankText:string;
            var suits = ["Invalid Suit","Clubs","Diamonds","Hearts","Spades"];

            switch(this._rank){
                case 0:
                    rankText = "Invalid Rank";
                    break;
                case 1:
                    rankText = "Ace";
                    break;
                case 11:
                    rankText = "Jack";
                    break;
                case 12:
                    rankText = "Queen";
                    break;
                case 13:
                    rankText = "King";
                    break;
                default:
                    rankText = this._rank.toString()
            }

            return rankText+" of "+suits[this._suit]+"("+this._rank.toString()+","+this._suit.toString()+")";
        }

    }

}