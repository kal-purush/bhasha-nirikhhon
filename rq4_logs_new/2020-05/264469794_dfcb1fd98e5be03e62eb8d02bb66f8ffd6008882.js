import React, { Fragment } from 'react';
import './App.css';
import InputTodo from "./components/InputTodo";
import ListTodo from "./components/ListTodo";
// import EditTodo from "./components/EditTodo";

function App() {
  return (
    <Fragment>
      <div className="container">
        <InputTodo />
        <ListTodo />
      </div>
    </Fragment>
  );
}

export default App;