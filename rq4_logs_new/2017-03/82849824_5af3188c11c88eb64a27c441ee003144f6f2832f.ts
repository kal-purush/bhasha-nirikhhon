import { Component, OnInit, OnDestroy } from '@angular/core';

import { GiphyEvents } from "../../modules/core/events/GiphyEvents";
import { TweetState } from "../../modules/core/state/TweetState";
import { GiphyState } from "../../modules/core/state/GiphyState";
import { TweetStateUpdates } from "../../modules/core/state/TweetStateUpdates";
import { GiphyStateUpdates } from "../../modules/core/state/GiphyStateUpdates";
import * as Models from '../../modules/core/models/Models';

@Component({
  moduleId: module.id,
  selector: 'giphy',
  templateUrl: 'giphy.component.html'
})
export class GiphyComponent implements OnInit, OnDestroy {

	trimmedTweetText: string;
	giphy: Models.GiphyImage;
	
	constructor(
		private giphyEvents: GiphyEvents,
		private tweetStateUpdates: TweetStateUpdates,
		private giphyStateUpdates: GiphyStateUpdates
	) {}

	ngOnInit() {
		this.tweetStateUpdates.subject.subscribe(this.onTweetStateUpdate.bind(this));
		this.giphyStateUpdates.subject.subscribe(this.onGiphyStateUpdate.bind(this));
	}

	ngOnDestroy() {
		this.tweetStateUpdates.subject.unsubscribe();
		this.giphyStateUpdates.subject.unsubscribe();
	}

	onTweetStateUpdate(state: TweetState) {
		if (state.trimmedTweetText && this.trimmedTweetText !== state.trimmedTweetText) {
			this.trimmedTweetText = state.trimmedTweetText;
			this.giphyEvents.requests.getGiphy.next("donald trump " + this.trimmedTweetText);
		}
	}

	onGiphyStateUpdate(state: GiphyState) {
		if (state && state.giphy && state.giphy.images && state.giphy.images.fixed_width) {
			this.giphy = state.giphy.images.fixed_width;
		}
	}
}