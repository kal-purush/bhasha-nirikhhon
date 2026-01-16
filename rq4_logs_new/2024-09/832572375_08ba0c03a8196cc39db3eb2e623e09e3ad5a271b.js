const initialState = {
  currentPlayer: "x",
  isGameEnded: false,
  isDraw: false,
  field: ["", "", "", "", "", "", "", "", ""],
};

export const reducer = (state = initialState, action) => {
  switch (action.type) {
    case "changeCurrentPlayer":
      return { ...state, currentPlayer: action.payload.changePlayer };

    case "setIsGameEnded":
      return { ...state, isGameEnded: action.payload.isGameEnded };

    case "setIsDraw":
      return { ...state, isDraw: action.payload.isDraw };

    case "setField":
      return { ...state, field: action.payload.setField };

    case "restartGame":
      return { initialState };

    default:
      return state;
  }
};