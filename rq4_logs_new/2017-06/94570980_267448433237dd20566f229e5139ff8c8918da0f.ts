import { Injectable } from '@angular/core';
import {Observable} from 'rxjs/Observable';

@Injectable()
export class TestCaseStatusService {
  public data: Observable<(string | number)[][]>;

  constructor() {
    this.data = new Observable(observer => {
      let delay = this.getRandomInt(100, 500);
      setTimeout(() => {
        console.log('Data Produced!');
        observer.next([['Failed', this.getRandomInt(1, 20)],
          ['Skipped', this.getRandomInt(1, 20)],
          ['Passed', this.getRandomInt(1, 20)],
          ['Error', this.getRandomInt(1, 20)]
        ]);
      }, delay);
      });
  }

  private getRandomInt(min, max) {
    //The maximum is exclusive and the minimum is inclusive
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min)) + min;
  }
}