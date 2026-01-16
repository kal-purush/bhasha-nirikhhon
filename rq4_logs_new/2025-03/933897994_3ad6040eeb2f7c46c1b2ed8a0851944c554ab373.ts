import { defineStore } from "pinia";
import { State, States, PLAYER_STATE_PROPS } from "./types";

export const usePlayerStore = defineStore("player", {
  state: (): State => ({
    playerElement: undefined,
    lastFrameProgress: undefined,
    state: States.PAUSED,
    currentProgress: 0,
    timeJump: false,
  }),
  actions: {
    load() {
      this.playerElement = document.getElementById(
        "player0",
      ) as HTMLVideoElement;
      setInterval(() => this.getPlayerStates(), 500);
    },
    getPlayerState(stateName: PLAYER_STATE_PROPS): boolean | number | string {
      return this.playerElement![stateName];
    },
    getPlayerStates() {
      const LIMIT_DELTA_TIME = 3;
      const [paused, currentProgress]: [boolean, number] = [
        this.getPlayerState(PLAYER_STATE_PROPS.PAUSED) as boolean,
        this.getPlayerState(PLAYER_STATE_PROPS.CURRENT_TIME) as number,
      ];

      this.lastFrameProgress = this.lastFrameProgress || currentProgress;

      const timeJump: boolean =
        Math.abs(currentProgress - this.lastFrameProgress) > LIMIT_DELTA_TIME;
      const state: States = paused ? States.PAUSED : States.PLAYING;

      this.lastFrameProgress = currentProgress;
      this.state = state;
      this.currentProgress = currentProgress;
      this.timeJump = timeJump;
    },
    setCurrentTime(time: number) {
      return (this.playerElement![PLAYER_STATE_PROPS.CURRENT_TIME] = time);
    },
    play() {
      this.playerElement!.play();
    },
    pause() {
      this.playerElement!.pause();
    },
  },
});