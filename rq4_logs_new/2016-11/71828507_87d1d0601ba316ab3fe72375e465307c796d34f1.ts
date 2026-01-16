import { Component, OnInit, Input, Output, EventEmitter } from '@angular/core';

import { TweetService } from './tweet.service';
import { Tweet } from './tweet.model';

@Component({
    selector: 'app-tweet-form',
    templateUrl: 'app/tweet/tweet-form.component.html'
})
export class TweetFormComponent implements OnInit {
    @Input() content: string;
    @Output() tweetAdded = new EventEmitter();

    constructor(private tweetService: TweetService) { }

    ngOnInit() { }

    addTweet() {
        this.tweetService.addTweet(this.content).then((tweet: Tweet) => {
            this.tweetAdded.emit(tweet);
            this.content = '';
        });
    }
}