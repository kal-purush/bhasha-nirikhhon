interface cardValue{
  card: Card;
  value: number;
}

class game{
  private name: string;
  private cardValues: Array<cardValue>;

  constructor(name, cardValues){
    this.name = name;
    this.cardValues = cardValues;
  }


  //array of functions to be executed to assign values
  /*
  usage:
  game.conditions.push(function(bar){alert(bar)})
  game.executeConditions()
  */
  public conditions: {
    (data: string): void; }[] = [];

    executeConditions() {
        for (var i=0; i < this.conditions.length; i++) {
             this.conditions[i]("test");
        }
    }

}