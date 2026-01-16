import { defineStore } from "pinia";

export interface IPrayerItem {
  english: string;
  time: string;
  arabic: string;
  passed: boolean;
  isNext: boolean;
  index: number;
}

export interface IUsePrayerStoreState {
  prayers: IPrayerItem[];
  nextPrayerIndex: number;
}

export const usePrayerStore = defineStore("prayers", {
  state: (): IUsePrayerStoreState => ({
    prayers: [],
    nextPrayerIndex: -1
  })
});