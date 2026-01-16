import React from "react";

//import Sidebar from "../Sidebar/Sidebar";
import MainDash from "../MainDash/MainDash";
import RightSide from "../RightSide/RightSide";
import Navigation from "../Navigation/Navigation";

const Global = () => {
  return (
    <div>
      <Navigation />
      <MainDash />
      <RightSide />
    </div>
  );
};

export default Global;