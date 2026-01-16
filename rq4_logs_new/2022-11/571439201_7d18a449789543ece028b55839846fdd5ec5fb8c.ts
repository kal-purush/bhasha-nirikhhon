import { Component } from '@angular/core';
import { ethers } from 'ethers';

@Component({
  selector: 'app-root',
  templateUrl: './app.component.html',
  styleUrls: ['./app.component.scss'],
})
export class AppComponent {
  title = 'Example Title';
  myNumber = 42;

  lastBlockNumber: number | undefined;

  clicks = 0;

  constructor() {
    this.lastBlockNumber = 0;
    ethers
      .getDefaultProvider('goerli')
      .getBlock('latest')
      .then((block) => (this.lastBlockNumber = block.number));
  }

  countClick() {
    this.clicks++;
  }
}