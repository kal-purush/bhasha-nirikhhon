import { Component, EventEmitter } from 'angular2/core';
import { ArmyComponent } from './army.component';
import { Army } from './army.model';
import { EditArmyDetailsComponent } from './edit-army-details.component';
import { NewArmyComponent } from './new-army.component';
import {DonePipe} from './done.pipe';

@Component({
  selector: 'army-list',
  inputs: ['armyList'],
  outputs: ['onArmySelect'],
  pipes: [DonePipe],
  directives: [ArmyComponent, EditArmyDetailsComponent, NewArmyComponent],
  template: `
  <select (change)="onChange($event.target.value)">
    <option value="all">Show All</option>
    <option value="broken">Show Broken</option>
    <option value="notBroken" selected="selected">Not Broken</option>
  </select>
  <army-display *ngFor="#currentArmy of armyList | broken:filterBroken"
    (click)="armyClicked(currentArmy)"
    [class.selected]="currentArmy === selectedArmy"
    [army]="currentArmy">
  </army-display>
  <edit-army-details *ngIf="selectedArmy" [army]="selectedArmy">
  </edit-army-details>
  <new-army (onSubmitNewArmy)="createArmy($event)"></new-army>
  `
})
export class ArmyListComponent {
  public armyList: Army[];
  public onArmySelect: EventEmitter<Army>;
  public selectedArmy: Army;
  public filterBroken: string = "notBroken";
  constructor() {
    this.onArmySelect = new EventEmitter();
  }
  armyClicked(clickedArmy: Army): void {
    this.selectedArmy = clickedArmy;
    this.onArmySelect.emit(clickedArmy);
  }
  createArmy(tempArmy: string): void {
    this.armyList.push(
      new Army(tempArmy[0].unitName, tempArmy[0].unitGeneral, tempArmy[0].unitSize, this.armyList.length)
    );
  }
  onChange(filterOption) {
    this.filterBroken = filterOption;
  }
}