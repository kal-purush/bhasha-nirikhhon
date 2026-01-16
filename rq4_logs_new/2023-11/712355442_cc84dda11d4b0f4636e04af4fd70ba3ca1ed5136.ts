import { NgModule } from '@angular/core';
import { CommonModule } from '@angular/common';

import { SpeakersRoutingModule } from './speakers-routing.module';
import { SpeakersComponent } from './speakers/speakers.component';
import { SpeakerComponent } from './speaker/speaker.component';
import { SpeakerDetailComponent } from './speaker-detail/speaker-detail.component';


@NgModule({
  declarations: [
    SpeakersComponent,
    SpeakerComponent,
    SpeakerDetailComponent
  ],
  imports: [
    CommonModule,
    SpeakersRoutingModule
  ]
})
export class SpeakersModule { }