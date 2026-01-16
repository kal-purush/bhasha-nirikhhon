import React from "react";
import Netflix_data from "./Netflix_data";
import Netflix from './Netflix'

export default function NetflixLover() {
  return (
    <div>
      <Netflix
        imgsrc={Netflix_data[4].imgsrc}
        title={Netflix_data[4].title}
        series__name={Netflix_data[4].series__name}
        link={Netflix_data[4].link}
      />
    </div>
  );
}