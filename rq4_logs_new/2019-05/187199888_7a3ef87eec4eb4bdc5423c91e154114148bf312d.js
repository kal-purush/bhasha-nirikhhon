import React from "react";
import { Button } from "antd";
import AuthorizedComponent from "./components/AuthorizedComponent";

import logo from "./logo.svg";
import "./App.css";

const ButtonAuthHOC = AuthorizedComponent({
  component: Button,
  actionName: "unAuthorizedActionSample",
  unAuthorizedProps: { disabled: true }
});

const ButtonAuthHOCX = AuthorizedComponent({
  component: Button,
  actionName: "authorizedActionSample",
  unAuthorizedProps: { disabled: true }
});

const ButtonAuthProps = AuthorizedComponent({
  component: Button
});

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <p>
          Edit <code>src/App.js</code> and save to reload.
        </p>
        <a
          className="App-link"
          href="https://reactjs.org"
          target="_blank"
          rel="noopener noreferrer"
        >
          Learn React
        </a>
        <ButtonAuthHOC type="primary">
          Unauthorized (From Element HOC)
        </ButtonAuthHOC>
        <ButtonAuthProps
          actionName="unAuthorizedActionSample"
          unAuthorizedProps={{ disabled: true }}
          type="primary"
        >
          Unauthorized (From Props)
        </ButtonAuthProps>
        <ButtonAuthHOCX type="primary">
          Authorized (From Element HOC)
        </ButtonAuthHOCX>
        <ButtonAuthProps
          actionName="authorizedActionSample"
          unAuthorizedProps={{ disabled: true }}
          type="primary"
        >
          Authorized (From Props)
        </ButtonAuthProps>
      </header>
    </div>
  );
}

export default App;