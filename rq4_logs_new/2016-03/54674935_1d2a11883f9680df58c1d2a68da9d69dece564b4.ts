import { Component, EventEmitter } from 'angular2/core';

@Component({
  selector: 'artist-select',
  inputs: ['artistList'],
  outputs: ['onArtistSelect'],
  template: `
    <select (change)="changeArtist($event.target.value)">
      <option>All</option>
      <option *ngFor="#currentArtist of artistList">{{ currentArtist }}</option>
    </select>
  `
})
export class ArtistSelectComponent {
  public artistList: string[];
  public onArtistSelect: EventEmitter<string>;
  constructor(){
    this.onArtistSelect = new EventEmitter();
  }
  changeArtist(artist: string) {
    console.log('artist-select emit', artist);
    this.onArtistSelect.emit(artist);
  }
}