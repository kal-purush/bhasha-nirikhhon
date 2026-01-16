enum Status {Free, Used, Discarded};

class Suit {
  name: string;
  cards : string[];

}

class Deck {
  Gold : string[];
  Spade : string[];
  Cup : string[];
  Club :string[];

  constructor(){
    this.Gold = Array(10);
    this.Spade = Array(10)
    this.Cup = Array(10)
    this.Club = Array(10)

  }

  mark(card : Card, status : string = 'Used') : void{
    this[card.getSuit()][card.getNumber()] = status;
  }

  isMarked(card: Card) : boolean {
    return this[card.getSuit()][card.getNumber()].length > 0 ||
            this[card.getSuit()][card.getNumber()] === 'Free'
  }

  echo() : string {
    var ret = '';
    ret += '[ Gold: ' + this.Gold + ']';
    ret += '[ Spade: ' + this.Spade + ']';
    ret += '[ Cup: ' +this.Cup + ']';
    ret += '[Club: '+this.Club + ']';
    return ret;
  }
}