import { Injectable } from '@angular/core';
import { BrowserStack } from 'protractor/built/driverProviders';

@Injectable({
  providedIn: 'root'
})
export class OneRepMaxService {

  constructor() { }

  calculateOneRepMax(reps,weight): any {
    let newMaxRep;
    let percentOfMax=0;
    reps = parseInt(reps,0)
    
    switch(reps){      
      case 1:
        percentOfMax = 100;
        break;
      case 2:
        percentOfMax = 95;
        break;
      case 3:
        percentOfMax = 90;
        break;
      case 4:
        percentOfMax = 88;
        break;
      case 5:
        percentOfMax = 86;
        break;
      case 6:
        percentOfMax = 83;
        break;
      case 7:
        percentOfMax = 80;
        break;
      case 8:
        percentOfMax = 78;
        break;
      case 9:
        percentOfMax = 76;
        break;
      case 10:
        percentOfMax = 75;
        break;
      case 11:
        percentOfMax = 72;
        break;
      case 12:
        percentOfMax = 70;
        break;
      default:{
        if(reps>=13 && reps<=15){
          percentOfMax = 60;
        }else if (reps>=16 && reps<=25){
          percentOfMax = 50;
        }else if (reps>=26 && reps <=30){
          percentOfMax = 40;
        }else {
          percentOfMax = 30;
        }
      }
    }
    console.log("Percent of max: " + percentOfMax)
    return newMaxRep = ((weight*100)/percentOfMax).toFixed(2)
  }
}
