import React from "react";
import Logo from "../../../../Widgets/Logo";

const LocationSection = ({clr,marginTp}) => {
  return (
    <div style={{ marginTop: marginTp }}>
      <div style={{ display: "flex" }}>
        <Logo logoClass="fas fa-map-marker-alt" />
        <p style={{ marginLeft: "5px", fontStyle: "italic", fontSize: "14px",color:clr }}>
          St.Paul Church
        </p>
      </div>

      <div style={{ display: "flex" }}>
        <Logo logoClass="fas fa-location-arrow" />
        <p
          style={{
            marginLeft: "5px",
            fontStyle: "italic",
            fontSize: "14px",
            color: clr,
          }}
        >
          510 Ave,NE,WA68005
        </p>
      </div>
    </div>
  );
};

export default LocationSection;