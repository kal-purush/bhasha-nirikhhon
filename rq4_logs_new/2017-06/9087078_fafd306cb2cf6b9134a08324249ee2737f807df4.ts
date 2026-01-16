import { Injectable } from '@angular/core';
import { Http } from '@angular/http';
import 'rxjs/add/operator/map';

import { Storage } from '@ionic/storage';

import { Moment } from 'moment';
import * as moment from 'moment';

import { Timbrage } from '../../model/Timbrage';

@Injectable()
export class StorageProvider {

  public static readonly COLLECTION_NAME = "timbrages";

  constructor(public storage: Storage) {
  }

  public save(date: Moment, timbrages: Array<Timbrage>): Promise<any> {
    return this.saveOnKey(this.getKey(date), timbrages);
  }

  private getKey(date: Moment): string {
    return date.format("YYYY-MM-DD");
  }

  public saveOnKey(key: string, timbrages: Array<Timbrage>): Promise<any> {
    return this.storage.ready().then(() => {
      let data = new Array();
      timbrages.forEach(timbrage => data.push(timbrage.date));
      return this.storage.set(key, data);
    });
  }

  public find(dateRef: Moment): Promise<Array<Timbrage>> {
    let data = new Array();
    return this.storage.get(this.getKey(dateRef))
      .then((jsonData) => {
        return JSON.parse(jsonData);
      })
      .then((data) => {
        let timbrages = new Array();
        if (data) {
          data.forEach(date => timbrages.push(new Timbrage(date)));
        }
        return timbrages;
      });
  }
}