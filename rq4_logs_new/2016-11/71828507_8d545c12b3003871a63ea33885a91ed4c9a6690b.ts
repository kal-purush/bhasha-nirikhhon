import {Component} from '@angular/core';
import {AuthService} from '../auth/auth.service';
import {Router} from '@angular/router';

import {TweetService} from '../tweet/tweet.service';

@Component({
    selector: 'app-home-component',
    templateUrl: 'app/home/home.component.html'
})
export class HomeComponent {

    public content: string;

    constructor(private authService: AuthService,
                private router: Router,
                private tweetService: TweetService) {
    }

    addTweet(): void {
        this.tweetService.addTweet(this.content);
    }

    logout(): void {
        this.authService.logout();
        this.router.navigateByUrl('login');
    }
}