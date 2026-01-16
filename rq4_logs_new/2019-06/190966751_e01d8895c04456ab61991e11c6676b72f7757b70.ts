import { Component, OnInit, Input } from '@angular/core';
import { drugList } from '../data';

@Component({
  selector: 'app-drug-display',
  templateUrl: './drug-display.component.html',
  styleUrls: ['./drug-display.component.scss'],
})
export class DrugDisplayComponent implements OnInit {
  @Input() inputs;
  @Input() drugList;

  drugTypesList = new Set();  // list of drug types

  constructor() { }

  ngOnInit() {
    for (const drug of drugList) {
      this.drugTypesList.add(drug.type);
    }
  }

}