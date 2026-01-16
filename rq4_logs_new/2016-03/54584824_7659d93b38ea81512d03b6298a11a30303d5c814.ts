import { Component } from 'angular2/core';

@Component({
  selector: 'my-app',
  template: `
    <div class="container">
      <h1>Skeleton Angular2 App!</h1>
    </div>
  `
})
export class AppComponent {
  public kegs: Keg[];
  constructor(){
    this.kegs = [
      new Keg("Widmer Brewery", "Omission IPA", 127),
      new Keg("Rogue Brewery", "Arrogant Bastard Ale", 97),
      new Keg("10 Barrel", "Sinistor Black Ale", 20)
    ];
  }
  kegWasSelected(clickedKeg: Keg): void {
    console.log(clickedKeg);
  }
}

export class Keg {
  public pints: number = 127;
  constructor(public brewerName: string, public beerType: string, public kegLevel: number) {

  }
}