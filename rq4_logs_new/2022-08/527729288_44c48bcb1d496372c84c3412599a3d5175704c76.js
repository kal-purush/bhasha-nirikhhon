import { createContext, useEffect, useReducer } from "react";
import reducer from "./reducer";

const PokerContext = createContext();
const initialState = {
  decks: [],
  drawnCards: [],
};
export const Provider = ({ children }) => {
  const [state, dispatch] = useReducer(reducer, initialState);
  console.log("ttt");
  useEffect(() => {
    console.log(state);
  }, [state]);

  function poruka() {
    console.log("poruka iz konteksta");
  }

  return (
    <>
      <PokerContext.Provider value={{ state, poruka }}>
        {children}
      </PokerContext.Provider>
    </>
  );
};

export default PokerContext;