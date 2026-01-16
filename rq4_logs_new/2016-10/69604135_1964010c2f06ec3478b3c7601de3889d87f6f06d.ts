import {Component} from '@angular/core';
import {Pipe} from '@angular/core';
 import {PipeTransform} from '@angular/core'
import {AngularFire, FirebaseListObservable, AngularFireDatabase} from 'angularfire2';
import {NgModule} from '@angular/core';


//import {AppModule} from '.app.module';

@Pipe({name: 'values', pure: true})
 export class ValuesPipe implements PipeTransform {
 transform(value:any, args:any[] = null):any {
 if (value) {
 return Object.keys(value).map(key => value[key]);
 }
 }
 }

/*@Component({
  selector: 'home',
  templateUrl: '/home.component.html',
  providers: [AngularFire]

})*/

//directives: [NgGrid, NgGridItem],
//pipes: [ValuesPipe],

@Component({
 selector: 'home',
 templateUrl: './home.component.html',
 providers: [AngularFire]
 })


export class HomeComponent {
  private timelines:FirebaseListObservable<any[]>;

  private timeline_title = "";
  private timeline_description = "";

  constructor(private af:AngularFire) {

    try {
      this.timelines = af.database.list('/timelines');

    }catch(e){
      console.log('timelines could not be fetched'+e.toString());
    }

  }

  private addTimeline() {

    var timestamp = (new Date()).toString();

    this.timelines.push({
      created_date: timestamp, title: this.timeline_title, description: this.timeline_description,
      is_private: false
    });

    this.timeline_title = '';
    this.timeline_description ='';

  }

  private addEvent(timeline:any) {

    let localTimeline:any = this.af.database.list('/timelines/' + timeline.$key + '/datapoints');

    localTimeline.push({
      "time": timeline.event_date,
      "event": timeline.event_description
    });

    /*timeline.event_date ='';
     timeline.event_description='';*/

  }

  private editEvent(temp1:string, temp2:string) {

    console.log("temp stings in editEvent: "+ temp1 +', '+temp2);

    /*let localTimeline:any = this.af.database.list('/timelines/' + timeline.$key + '/datapoints');
     localTimeline.update({
     "event": timeline.event_description
     });*/

  }


}