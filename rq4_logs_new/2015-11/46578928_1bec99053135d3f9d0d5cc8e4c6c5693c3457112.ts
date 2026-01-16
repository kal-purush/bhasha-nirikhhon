import { Pipe, PipeTransform } from 'angular2/angular2'
import { ToObjectPipe } from './to_object_pipe'

@Pipe({
  name: 'toArray',
})

export class ToArrayPipe implements PipeTransform {
  transform(snapshot: FirebaseDataSnapshot, args: string[] = []): any[] {
    if (!snapshot || !snapshot.hasChildren()) {
      return null;
    }

    let toObjectPipe = new ToObjectPipe();
    var orderedArray = <any[]>[];

    snapshot.forEach(child => {
      orderedArray.push(toObjectPipe.transform(child));
    });

    return orderedArray;
  }
}