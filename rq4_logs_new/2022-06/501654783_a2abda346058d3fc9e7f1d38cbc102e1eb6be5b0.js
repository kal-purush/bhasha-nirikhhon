import React from "react";

function EsraaCard() {
  const card = {
    border: "1px solid #e1e1e1",
    width: "50%",
    margin: "auto",
    boxShadow: "-1px 4px 5px rgba(0,0,0,0.13)",
  };
  return (
    <div>
      <div style={card}>
        <img
          style={{ width: "25%", borderRadius: "50%" }}
          src="https://i.pinimg.com/736x/9d/1b/77/9d1b77e2a4468304372aa37692627d2a.jpg"
          alt="profile-img"
        />
        <h3>
          <strong>Name :</strong>Esraa ismail
        </h3>
        <h4>
          <strong>Age :</strong>27
        </h4>
      </div>
    </div>
  );
}

export default EsraaCard;