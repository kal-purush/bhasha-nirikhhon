import { Injectable, Pipe } from '@angular/core';
import { IAnnotation } from "../models/IPoem";

/*
  Generated class for the PoemBodyAnnotation pipe.

  See https://angular.io/docs/ts/latest/guide/pipes.html for more info on
  Angular 2 Pipes.
*/
@Pipe({
  name: 'poem-body-annotation'
})
@Injectable()
export class PoemBodyAnnotation {
  /*
    Takes a value and makes it lowercase.
   */
  transform(value, args) {
    var line : string  = value + ''; // make sure it's a string
    let anns:IAnnotation[] = args[0];
    for (var i=0;i<anns.length;i++)
    {
      if(i==args[1])
      {
        var parts = line.split(anns[i].word);
        line = parts[0] + "<sub>"+i+"</sub>" + parts[1];
      }
    };

    return line;
  }
}