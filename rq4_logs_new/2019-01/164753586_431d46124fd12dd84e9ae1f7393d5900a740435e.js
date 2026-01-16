import React, { Component } from "react";
import "./App.css";
import Form from "./components/Form";
import Recipes from "./components/Recipes";

class App extends Component {
  state = {
    recipes: []
  };

  getRecipe = async e => {
    e.preventDefault();
    const recipeName = e.target.elements.recipeName.value;
    const key = "ea4ee084f996b16e89821aeb4530850b";
    const api_call = await fetch(
      `http://cors-anywhere.herokuapp.com/https://www.food2fork.com/api/search?key=${key}&q=${recipeName}&count=10`
    );
    const data = await api_call.json();
    this.setState({
      recipes: data.recipes
    });
  };

  componentDidMount = () => {
    const json = localStorage.getItem("recipes");
    const recipes = JSON.parse(json);

    if (recipes === null) {
      this.setState({
        recipes: []
      });
    } else {
      this.setState({
        recipes: recipes
      });
    }
  };

  componentDidUpdate = () => {
    const recipes = JSON.stringify(this.state.recipes);
    localStorage.setItem("recipes", recipes);
  };

  render() {
    return (
      <div className="App">
        <header className="App-header">
          <h1 className="App-title">Recipe Search</h1>
        </header>
        <Form getRecipe={this.getRecipe} />
        <Recipes recipes={this.state.recipes} />
      </div>
    );
  }
}

export default App;