import { Injectable } from "src/withXeno/inversify";
import { action, makeObservable, observable } from "mobx";

@Injectable({
  singleton: true,
})
export class BoxManagerStore {
  constructor() {
    makeObservable(this, {
      currentBoxes: observable,
      removeBoxById: action.bound,
      generateBoxes: action.bound,
    });
  }
  nextBoxId = 11;
  currentBoxes = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10];

  removeBoxById = (id: number) => {
    this.currentBoxes = this.currentBoxes.filter((o) => o !== id);
    if (this.currentBoxes.length <= 2) {
      this.generateBoxes();
    }
  };

  generateBoxes = (count = 10) => {
    const ids = new Array(count).fill(0).map((_, idx) => this.nextBoxId + idx);
    this.nextBoxId += count;
    this.currentBoxes = this.currentBoxes.concat(ids);
  };
}