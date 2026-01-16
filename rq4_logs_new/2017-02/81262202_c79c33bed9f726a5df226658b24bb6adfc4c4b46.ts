import { Injectable } from '@angular/core';
import { SQLite } from 'ionic-native';

@Injectable()
export class DBProvider {

  constructor() {
    console.log('Hello DBProvider Provider');
  }

  dbInitialSetUp(): void {
    let db = new SQLite();
    db.openDatabase({
        name: "data.db",
        location: "default"
    }).then(() => {
        db.executeSql("CREATE TABLE IF NOT EXISTS favourites (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, author TEXT)", {})
          .then((data) => { 
                              console.log("TABLE CREATED: ", data);
                          }, (error) => {
                              console.error("Unable to execute sql", error);
                          })
    }, (error) => {
        console.error("Unable to open database", error);
    });
  }

}