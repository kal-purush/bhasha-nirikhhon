import {Pipe} from 'angular2/angular2';

export class DateFormat extends Pipe {
  supports(): boolean {
    return true;
  }

  transform(value): string {

    let date = new Date(value);
    return moment(date).format('DD/MM/YYYY');

  }

  create(): Pipe {
    return this;
  }
}