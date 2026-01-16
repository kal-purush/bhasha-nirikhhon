import { Component, Input } from '@angular/core';
import { Rating } from 'src/app/users/user';

@Component({
  selector: 'app-mat-rated-chips',
  templateUrl: './mat-rated-chips.component.html',
  styleUrls: ['./mat-rated-chips.component.scss']
})
export class MatRatedChipsComponent {

  @Input() items: any[] = [];
  @Input() key = '';

  constructor() { }

  getRatingColor(rating: Rating) {
    switch (rating) {
      case Rating.Intermediate:
        return {
          'intermediate': true
        };
      case Rating.Advanced:
        return {
          'advanced': true
        };

      default:
        return '';
    }
  }

}