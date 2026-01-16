import React from "react";
import { Provider } from 'react-redux';
import store from "../store";
import ShowData from "./show";

export default class App extends React.Component {
  render() {
    return (
      <Provider store={store}>
       <ShowData />
      </Provider>
    )
  }
}