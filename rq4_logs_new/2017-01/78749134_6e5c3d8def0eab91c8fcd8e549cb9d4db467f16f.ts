import { Component, Input } from '@angular/core';
// import { Item } from '../item';

@Component({
  selector: 'flag-filter',
  templateUrl: './flags-filter.component.html'
})

export class FlagFilterComponent {
  @Input() flags: string[];
  constructor() {}

  toggleFlag = function (flag: string) {
    console.log('shin');
    let arr = this.flags;
    let include = arr.includes(flag);
    if (include) {
      let index = arr.indexOf(flag);
      this.flags.splice(index, 1);
      console.log('yes', this.flags);
    } else {
      this.flags.push(flag);
      console.log('no', this.flags);
    }
  }
}