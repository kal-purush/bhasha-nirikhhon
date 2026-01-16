import { Injectable } from '@angular/core';
import {AngularFireDatabase, FirebaseListObservable, FirebaseObjectObservable} from 'angularfire2/database';

@Injectable()
export class AlarmListService {
  private dbPath = '/alarms';

  alarms: FirebaseListObservable<any[]>;
  alarm: FirebaseObjectObservable<any[]>;

  constructor(
    private db: AngularFireDatabase,
  ) { }

  getAlarmList(query = {}): FirebaseListObservable<any[]> {
    this.alarms = this.db.list(this.dbPath, {
      query: query
    });
    return this.alarms;
  }

  getAlarm(key: string): FirebaseObjectObservable<any[]> {
    this.alarm = this.db.object(`${this.dbPath}/${key}`);
    return this.alarm;
  }

}