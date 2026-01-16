import { Component, OnInit } from '@angular/core';

import { StellarBody } from './stellarbody';
import { StellarBodyService } from './stellarbody.service';

@Component({
  selector: 'app-stellarbodies',
  templateUrl: './stellarbodies.component.html',
  styleUrls: ['./stellarbodies.component.css']
})
export class StellarBodiesComponent implements OnInit {
  title = '';
  stellarBodies: StellarBody[];
  selectedStellarBody: StellarBody;

  constructor(private stellarBodyService: StellarBodyService) {}

  getStellarBodies(): void {
    this.stellarBodyService.getStellarBodies().then(stellarBodies => this.stellarBodies = stellarBodies);
  }

  ngOnInit(): void {
    this.getStellarBodies();
  }
}