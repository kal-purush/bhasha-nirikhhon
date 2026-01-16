import {Http} from 'angular2/http';
import {Injectable} from 'angular2/core';
import {Rune} from './Rune';


@Injectable()
export class RuneService {
  public stats: Object;
  public runes: Rune[] = [];
  constructor(public http: Http) { }

  getRunes() {
    this.http.get('assets/app/data/en/runes-lang.json')
      .map(res => res.json())
      .subscribe(
        data => {
          this.stats = data.stats;
          this.runes = data.runes;
        },
        error => console.log(error),
        () => console.log('Done!')
      );
  }
}