import React from "react";

const Card = ({ image, title, excerpt, isEmpty }) => {
  return (
    <div className={`card ${isEmpty ? "empty" : ""}`}>
      <div className="card-image-container">
        {image ? (
          <img src={image} alt={title} className="card-image" />
        ) : (
          <div className="empty-overlay"></div> // ✅ Fond blanc avec overlay gris
        )}
      </div>
      <div className="card-content">
        <h3>{title}</h3>
        <p>{excerpt}</p>
      </div>
    </div>
  );
};

export default Card;