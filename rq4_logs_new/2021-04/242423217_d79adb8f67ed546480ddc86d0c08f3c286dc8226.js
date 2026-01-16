import "./App.css";
import UIForm from "./components/UIForm";
import React from "react";

class FullForm extends React.Component {
  render() {
    return (
      <div className="FullForm">
        <link
          rel="stylesheet"
          href="https://fonts.googleapis.com/css?family=Roboto:300,400,500,700&display=swap"
        />
          <UIForm />
      </div>
    );
  }
}

export default FullForm;