import { defineStore } from "pinia";

export const useAttackStore = defineStore("AttackStore", {
  state: () => {
    return {
      drawObjects: [],
    };
  },
});