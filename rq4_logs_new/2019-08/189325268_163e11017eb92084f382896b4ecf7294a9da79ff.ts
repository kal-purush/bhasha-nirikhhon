import { NgModule } from '@angular/core';

import { FontAwesomeModule } from '@fortawesome/angular-fontawesome';
import {
  faStar, faThumbsDown, faHandPaper, faSortAmountUp, faUsers, faChartLine,
  faPlusSquare, faSignOutAlt, faDice, faSignInAlt, faUserPlus
} from '@fortawesome/free-solid-svg-icons';
import { library } from '@fortawesome/fontawesome-svg-core';

@NgModule({
  imports: [
    FontAwesomeModule
  ]
})
export class MockFontAwesomeModule {
  constructor() {
    library.add(faStar, faThumbsDown, faHandPaper, faSortAmountUp, faUsers, faChartLine, faPlusSquare, faSignOutAlt, faDice, faSignInAlt, faUserPlus);
  }
}