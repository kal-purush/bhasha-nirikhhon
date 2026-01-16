import { Component, Input, OnInit } from '@angular/core';
import { SeqType, Story } from '../../../../models/story.model';
import { StoriesService } from '../../../../services/stories.service';
import { Platform } from '@ionic/angular';
import { Browser } from '@capacitor/core';
import { Router } from '@angular/router';
// import { MapInfoUIService } from '../../../../services/map-info-ui.service';

@Component({
  selector: 'utm-marker-popup',
  templateUrl: './marker-popup.component.html',
  styleUrls: ['./marker-popup.component.scss'],
})

export class MarkerPopupComponent implements OnInit {
  @Input() story: Story;
  sources: any[];

  constructor(
     stories: StoriesService,
    public platform: Platform,
    public router: Router,
    // public mapInfoUI: MapInfoUIService
    ) { 
      this.sources = [ {
          src: 'http://static.videogular.com/assets/audios/videogular.mp3',
          type: 'audio/mp3',
        } ]
      }

  ngOnInit() {}

  startStory(story: Story) {
    // TODO: Implement actual sequence logic here
    // For now, only load first sequence item
    const firstSeqItem = story.seq[0];
    switch (firstSeqItem['@type']) {
      // case SeqType.Article:
      //   this.router.navigate([
      //     '/article',
      //     { storyId: story['@id'], seqId: firstSeqItem['@id'] },
      //   ]);
      //   break;
      case SeqType.Dialogue:
        this.router.navigate([
          '/dialogue',
          { storyId: story['@id'], seqId: firstSeqItem['@id'] },
        ]);
        break;
      // case SeqType.External:
      //   Browser.open({ url: firstSeqItem['content'] });
      //   break;
      // case SeqType.TimeSlider:
      //   this.router.navigate([
      //     '/timeslider',
      //     { storyId: story['@id'], seqId: firstSeqItem['@id'] },
      //   ]);
      //   break;
      default:
        console.error('Unsupported story type', firstSeqItem['@type']);
        break;
    }
  }

}