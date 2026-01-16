import { makeScene } from "./normalized3Dcords";
import React from "react";
import "./App.css";

const App = () => {
  React.useEffect(() => {
    // console.log(utils);
    makeScene();
    
  }, []);

  // console.log(imgd);

  return null;
};

export default App;