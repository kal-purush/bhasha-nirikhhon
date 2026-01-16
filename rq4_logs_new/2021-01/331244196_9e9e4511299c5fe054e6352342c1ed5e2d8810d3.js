import React from "react";
import ReactDom from "react-dom";
import Card from "./Card";
import SData from "./SData";
import "./index.css";

ReactDom.render(
    <>

    <h1 className="heading_style"> List of Top 5 Netflix Series</h1>
       
       {SData.map((curVal)=>
        <Card 
        imgsrc={curVal.imgsrc}
        alt={curVal.alt}
        title={curVal.title}
        sname={curVal.sname}
        link={curVal.link}
      />
       )} 
        
</>, document.getElementById("root"));



