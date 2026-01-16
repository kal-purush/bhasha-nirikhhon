import { Component, OnInit } from '@angular/core';
import { Tweet } from './../models/tweet.model';
import { UserInfoComponent } from './../user-info/user-info.component'
import { TimelineService } from '../timeline.service';
import { FirebaseListObservable } from 'angularfire2/database';
import { LikeService } from '../like.service';



@Component({
  selector: 'app-timeline',
  templateUrl: './timeline.component.html',
  styleUrls: ['./timeline.component.css'],
  providers: [TimelineService]
})
export class TimelineComponent implements OnInit {
  constructor(private timelineService: TimelineService) { }
  ngOnInit() {
    this.tweets = this.timelineService.getTweets();
  }
  tweets: FirebaseListObservable<any[]>;
  timeline: Tweet[];


  makeTweet(tweet: string){
    var newTweet = new Tweet('Ami', 'ik4rus', './assets/usericon.png',tweet,'',0,0,0);
    this.timelineService.postTweet(newTweet);
  }
}