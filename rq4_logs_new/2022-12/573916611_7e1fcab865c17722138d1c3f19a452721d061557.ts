import { IS_BROWSER } from "https://deno.land/x/fresh@1.1.2/runtime.ts";

export interface SoundPlayer {

  preload(sound: string): void;

  play(sound: string): Promise<void> | void;
}

export class SoundManager implements SoundPlayer {
  private players: { [sound: string]: HTMLAudioElement } = {}
  private currentPlayer: HTMLAudioElement | null = null
  private resolveCurrentPlayerDone: (() => void) | null = null

  preload(sound: string) {
    if (sound in this.players) return;

    const player = this.players[sound] = new Audio(sound);

    player.onpause = () => {
      if (this.currentPlayer == player && this.resolveCurrentPlayerDone) {
        this.resolveCurrentPlayerDone();
        this.resolveCurrentPlayerDone = null;
      }
    }
  }

  play(sound: string) {
    this.preload(sound);

    if (this.currentPlayer) {
      this.currentPlayer.pause();
      if (this.resolveCurrentPlayerDone) this.resolveCurrentPlayerDone();
    }

    const player = this.players[sound];
    player.currentTime = 0;
    
    player.play();
    this.currentPlayer = player;
    
    return new Promise<void>((resolve) => {
      this.resolveCurrentPlayerDone = resolve;
    });
  }
}

export const mockSoundManager: SoundPlayer = {
  preload() {},
  play() {}
};

export const soundManager = IS_BROWSER ? new SoundManager() : mockSoundManager;