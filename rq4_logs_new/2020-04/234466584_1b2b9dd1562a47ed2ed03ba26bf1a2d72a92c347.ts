import { Injectable } from '@angular/core';
import { BehaviorSubject } from 'rxjs';
import { Researcher } from './models/researcher.model';
import { Measure } from './models/measure.model';

@Injectable()
export class ResearchSharedService {
    researcher1: Researcher = new Researcher('m12345678','John','Snoe','snoej@mail.uc.edu','Wall Tending',[new Measure('1a5fdbaa-c645-4c74-be09-f75377ece388','2020-04-17','227000','210686.4','100000','13139433.3','157','1072443.5','d18d456b-6860-475e-96cd-6965d0747855','Sum of Award Amount','SUM','a18d456b-6860-475e-96cd-6965d0747857','Average'),new Measure('41aac5cc-5714-44e5-ad04-1afb5481faa1','2020-04-17','227000','210686.4','150000','1027958','14010','243359.8','d18d456b-6860-475e-96cd-6965d0747856','Sum of Award Amount For NSF','SUM','b18d456b-6860-475e-96cd-6965d0747857','Average')]);
    private researcher = new BehaviorSubject(this.researcher1);
    sharedResearcher= this.researcher;

  constructor() { }

  nextResearcher(researcher: Researcher) {
    this.researcher.next(researcher)
  }
  
}