import React from "react";
import { StyleSheet, Text, View } from "react-native";
import TodoApp from "./src/TodoApp";
import {Provider} from "react-redux";
import store from "./src/store";

export default function App() {
  return (
    <Provider store={store}>
      <TodoApp />
    </Provider>
  );
}