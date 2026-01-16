import {Component, OnInit} from 'angular2/core';
import {HTTP_PROVIDERS} from 'angular2/http';
import {Observable} from 'rxjs/Observable';
import 'rxjs/add/observable/forkJoin';

import {GitHubService} from './github.service';

@Component({
  selector: 'github-profile',
  styles: [
    `
      .avatar {
        width: 100;
        height: 100;
        border-radius: 100%;
      }
    `
  ],
  template: `
    <i *ngIf="isLoading" class="fa fa-spinner fa-spin fa-3x"></i>
    <h2>@{{ user.login }}</h2>
    <img src="{{ user.avatar_url }}" class="avatar" />

    <h3>Followers</h3>
    <div *ngFor="#follower of followers" class="media">
      <div class="media-left">
        <a href="">
          <img src="{{ follower.avatar_url }}" class="media-object avatar" />
        </a>
      </div>
      <div class="media-body">
        <h4 class="media-heading">{{ follower.login }}</h4>
      </div>
    </div>
  `,
  providers: [HTTP_PROVIDERS, GitHubService]
})

export class GitHubProfileComponent implements OnInit {
  isLoading = true;
  username = "octocat";
  user = {};
  followers = [];

  constructor(private _gitHubService: GitHubService) {

  }

  ngOnInit() {
    Observable.forkJoin(
      this._gitHubService.getUser(this.username),
      this._gitHubService.getFollowers(this.username)
    )
    // subscribe to that Observable
    .subscribe(
      // get the result
      res => {
        // set the properties of this component
        this.user = res[0];
        this.followers = res[1];
      },
      // error handler - didn't add it to not pollute it
      null,
      // handler for completion of the Observable
      () => { this.isLoading = false; 
    })
  }
}