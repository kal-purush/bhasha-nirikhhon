import { Injectable } from '@angular/core';
import { TweeterUser } from '../models/tweeter-user';
import { ActivatedRouteSnapshot, Resolve, RouterStateSnapshot} from '@angular/router';
import { Observable } from 'rxjs';
import {TwitterService} from '../services/twitter.service';

@Injectable({
  providedIn: 'root'
})
export class TwitterResolverService implements Resolve <TweeterUser[]> {

  constructor(private twitterService: TwitterService) { }

  resolve(route: ActivatedRouteSnapshot, state: RouterStateSnapshot):
    Observable<TweeterUser[]> | Promise<TweeterUser[]> | TweeterUser[] {
    return this.twitterService.onlyDePaulSocialHashTagTweets();
  }
}