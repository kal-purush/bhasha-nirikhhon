import { createReducer } from "@reduxjs/toolkit";
import { shuffleArray } from "../../utils";
import { discardPolicy, playPolicy } from "./actions";
import createPolicyStack from "./createPolicyStack";
import Policy from "./Policy";

export interface PolicyDeckReducer {
  drawingPile: Policy[];
  discardPile: Policy[];
  nLiberalsPlayed: number;
  nFascistsPlayed: number;
}

function getInitialState(): PolicyDeckReducer {
  return {
    drawingPile: createPolicyStack(),
    discardPile: [],
    nLiberalsPlayed: 0,
    nFascistsPlayed: 0,
  };
}

export default createReducer(getInitialState(), (builder) => {
  builder
    .addCase(discardPolicy, (state, { payload }) => {
      state.discardPile.push(state.drawingPile[payload]);
      state.drawingPile.splice(payload, 1);
    })
    .addCase(playPolicy, (state) => {
      const cardToPlay = state.drawingPile[0];
      if (cardToPlay === "liberal") {
        state.nLiberalsPlayed += 1;
      } else {
        state.nFascistsPlayed += 1;
      }
      state.drawingPile.splice(0, 1);
      if (state.drawingPile.length < 3) {
        state.drawingPile = shuffleArray([
          ...state.drawingPile,
          ...state.discardPile,
        ]);
        state.discardPile = [];
      }
    });
});