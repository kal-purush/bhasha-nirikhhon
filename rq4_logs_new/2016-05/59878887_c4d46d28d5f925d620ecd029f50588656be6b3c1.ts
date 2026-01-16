import { Component } from 'angular2/core';
import { ControlGroup, FormBuilder } from 'angular2/common';
import {Observable} from 'rxjs/Rx';

import 'rxjs/add/operator/debounceTime';
import 'rxjs/add/operator/map';

@Component({
  selector: 'adventure-form',
  template: `
    <form [ngFormModel]="form">
      <input type="text" ngControl="search" />
    </form>
  `,

})

export class AdventureFormComponent {
  form: ControlGroup
  

  constructor(fb: FormBuilder) {
    this.form = fb.group({
      search: []
    })

    var search = this.form.find('search');
    search.valueChanges
      .debounceTime(400)
      .map(str => (<string>str).replace(' ', '-'))
      .subscribe(x => console.log(x));

    var observable = Observable.fromArray([1, 2, 3]);

    var startDates = [];
    var startDate = new Date();

    for (var day = -2; day <= 2; day++) {
      var date = new Date(
        startDate.getFullYear(),
        startDate.getMonth(),
        startDate.getDate() + day
      );

      startDates.push(date);
    }

    Observable
      .fromArray(startDates)
      .map(date => {
        console.log("Getting deals for date" + date);
        return [1, 2, 3]; //return data to the subscribe
      })
      .subscribe(x => console.log(x));

    // var timerObservable = Observable.interval(1000); // call to the server to get tweets emails etc every minute
    // timerObservable
    //   .map(x => {
    //     console.log("calling the server to get the latest news");
    //   })
      // .flatMap( x => {
      //   console.log("calling the server to get the latest news");
      //   return observable.of([1,2,3])
      //  })
      // .subscribe(news => console.log(news));

      // RUNNING PARALLEL - grabbing two items and return them together
    var userStream = Observable.of({
      userId: 1, username: 'sam'
    }).delay(2000);

    var tweetsStream = Observable.of([1, 2, 3]).delay(1500);

    // Observable
    //   .forkJoin(userStream, tweetsStream)
    //   .subscribe(result => console.log(result));

    Observable
      .forkJoin(userStream, tweetsStream)
      .map(joined => new Object({ user: joined[0], tweets: joined[1] })) // map into data structure that app expects
      .subscribe(result => console.log(result));

    // failed observable
    var failObservable = Observable.throw(new Error("Something failed."));
    failObservable.subscribe(
      x => console.log(x),
      error => console.error(error) 
    );

  }


}