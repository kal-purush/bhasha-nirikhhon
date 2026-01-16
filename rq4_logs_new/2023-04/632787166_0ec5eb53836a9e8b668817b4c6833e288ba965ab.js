import "./App.css";
import Card from "./components/Card";
import Content from "./components/Content";
import Header from "./components/Header";
import Overlay from "./components/Overlay";
import React from "react";
import axios from "axios";
export const AppContext = React.createContext({});

function App() {
  const [cartOpened, setCartOpened] = React.useState(false);
  const [items, setItems] = React.useState([]);
  const [searchValue, setSearchValue] = React.useState([]);
  const onChangeSearchInput = (event) => {
    setSearchValue(event.target.value);
  };
  const [cartItems, setCartItems] = React.useState([
    {
      name: "Air Jordan 4 Retro PSG Paris Saint",
      price: 25000,
      imagUrl: "/img/sneakers/1.jpg ",
    },
    {
      name: "Air Jordan 1 Low Court Purple",
      " price": 29000,
      imagUrl: "/img/sneakers/2.jpg",
    },
    {
      name: "Air Jordan 4 Retro Metallic Red",
      price: 21000,
      imagUrl: "/img/sneakers/3.jpg",
    },
    {
      name: "Air Jordan 4 Retro Metallic Red",
      price: 21000,
      imagUrl: "/img/sneakers/3.jpg",
    },
  ]);

  React.useEffect(() => {
    axios
      .get("https://63e3c485c919fe386c0e6ec4.mockapi.io/items")
      .then((res) => {
        setItems(res.data);
      });
  });

  const addToCart = (obj) => {
    axios.post("https://63e3c485c919fe386c0e6ec4.mockapi.io/cart");
    setCartItems((prev) => [...prev, obj]);
  };

  return (
    <AppContext.Provider value={{ items, cartOpened, searchValue, cartItems }}>
      <div className="wrapper">
        <Header onClickCart={() => setCartOpened(true)}></Header>
        {cartOpened ? (
          <Overlay items={cartItems} onClose={() => setCartOpened(false)} />
        ) : null}
        <div className="cardsDisplay">
          {items
            .filter((item) => item.name.toLowerCase().includes(searchValue))
            .map((obj, index) => (
              <Card
                key={index}
                title={obj.name}
                price={obj.price}
                imagUrl={obj.imagUrl}
              ></Card>
            ))}
        </div>
      </div>
    </AppContext.Provider>
  );
}

export default App;