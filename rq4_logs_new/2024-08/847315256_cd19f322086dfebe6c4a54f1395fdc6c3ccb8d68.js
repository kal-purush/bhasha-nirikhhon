import { createContext, useContext, useState } from "react";
import React from "react";

const StateContext = createContext();

// initialState = {
//   chat: false,
//   cart: false,
//   userProfile: false,
//   notification: false,
// };

export const ContextProvider = ({ children }) => {
  const [activeMenu, setActiveMenu] = useState(true);
  return (
    <StateContext.Provider value={{ activeMenu, setActiveMenu }}>
      {children}
    </StateContext.Provider>
  );
};

export const useStateContext = () => useContext(StateContext);