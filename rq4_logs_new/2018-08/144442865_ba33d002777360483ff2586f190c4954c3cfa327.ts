import { Injectable } from '@angular/core';
import { Storage } from '@ionic/storage';
// import { IonicPage, ModalController, NavController, NavParams } from 'ionic-angular';

/*
  Generated class for the TaskProvider provider.

  See https://angular.io/guide/dependency-injection for more info on providers
  and Angular DI.
*/
@Injectable()
export class Task {

  public taskData:any;
  constructor(public taskStorage: Storage) {
  }

  load() {

  }

  async addTask(key, taskData) {
    console.log("adding Key: ", key);
    console.log("with data : ", taskData);
    console.log("with title : ", taskData[0].title);
    console.log("with desc : ", taskData[0].description);
    console.log("with compl : ", taskData[0].completed);

    this.taskStorage.ready().then(() => {
      this.taskStorage.set(key, {
        title: taskData[0].title,
        description: taskData[0].description,
        completed: taskData[0].completed
      });
    });
  }

  async displayTask(key) {
    console.log('getting task for Key : ', key);

    return await this.taskStorage.ready().then(() => {
      this.taskStorage.get(key).then((data) => {
        console.log("retrieved data : ", data);
        return data;
      });
    });
  }
}