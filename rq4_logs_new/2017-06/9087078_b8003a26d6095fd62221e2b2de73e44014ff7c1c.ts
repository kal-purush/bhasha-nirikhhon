import { Injectable } from '@angular/core';
import 'rxjs/add/operator/map';

import { Timbrage } from '../../model/Timbrage';
import { Moment, Duration } from 'moment';
// import { extendMoment } from 'moment-range';
// const moment = extendMoment(Moment);

@Injectable()
export class CalculationProvider {

  constructor() {
    console.log('Hello CalculationProvider Provider');
  }

  public calculate(timbrages: Array<Timbrage>): Duration {
    return null; //new Moment(timbrages[0].getDate()).diff(timbrages[1].getDate()).duration();
  }
}