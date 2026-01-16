import React from "react";
import PropTypes from "prop-types";
import ShopCard from "../ShopCard";

export default function CardsView({ cards }) {
  if (!cards.length) return null;
  return (
    <div className="cards-view">
      {cards.map((item, index) => {
        return <ShopCard cards={item} key={index} />;
      })}
    </div>
  );
}

CardsView.defaultProps = {
  cards: [
    {
      name: "Nike Metcon 2",
      price: "130",
      color: "red",
      img:
        "https://raw.githubusercontent.com/netology-code/ra16-homeworks/master/events-state/layouts/img/1.jpg"
    }
  ]
};

CardsView.propTypes = {
  cards: PropTypes.array.isRequired
};