import FoodItem from "./FoodItem";

const Favorites = ({ favorites }) => {
  return (
    <ul className="favorites">
      {favorites.map((item, index=Date.now()) => <FoodItem src={item} key={index} />)}
    </ul>
  );
};

export default Favorites;