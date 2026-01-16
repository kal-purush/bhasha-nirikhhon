import { Injectable } from '@angular/core';

declare var audio_mode;

@Injectable()
export class AudioModeProvider {

	constructor() {
	}

  updateAudioMode(mode: String) {
    audio_mode.change_audio_mode(mode, ()=>{}, ()=>{});
  }

}