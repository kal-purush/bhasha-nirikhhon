import { Component, EventEmitter } from 'angular2/core';


/////Keg List////
@Component({
  selctor: 'keg-list',
  inputs: ['kegList'],
  outputs: ['onTaskSelect'],
  directives: [KegComponent],
  templateUrl: 'app/keg-list.view.html'
})

export class KegListComponent {
  public kegList: Keg[];
  public onKegSelect: EventEmitter<Keg>;
  public selectedKeg: Keg;
  public filterType: string = "";
  constructor(){
    this.onKegSelect = new EventEmitter();
  }
  kegClicked(clickedKeg: Keg): void {
    console.log('child', clickedKeg);
    this.selectedKeg = clickedKeg;
    this.onKegSelect.emit(clickedKeg);
  }
  createKeg(brewerName: string, beerName: string, beerType: string): void {
    this.kegList.push(
      new Keg(brewerName, beerName, beerType)
    );
  }
  onChange(filterOption){
    this.filterType = filterOption;
    console.log(this.filterType);
  }
}