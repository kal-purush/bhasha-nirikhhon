import initialGameState from "./initialStateGame";
import { wantDelGame, doNotWantDelGame } from "./actionTypesGames";

const GamesReducer = (
  state = initialGameState,
  action: { type: string; payload: string }
): { gameWantToDelete: string } => {
  switch (action.type) {
    case wantDelGame:
      return {
        ...state,
        gameWantToDelete: action.payload,
      };
    case doNotWantDelGame:
      return {
        ...state,
        gameWantToDelete: initialGameState.gameWantToDelete,
      };
    default:
      return state;
  }
};

export default GamesReducer;