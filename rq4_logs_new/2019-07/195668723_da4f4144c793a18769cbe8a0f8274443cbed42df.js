import React from 'react';
import { Provider } from 'react-redux';
import './App.css';
import Products from "./products";
import createStore from "./store/createStore";
import Filters from "./filters";

const store = createStore();

function App() {
    return (
      <Provider store={store}>
          <div className="App">
              <Filters/>
              <Products/>
          </div>
      </Provider>
    );
}

export default App;