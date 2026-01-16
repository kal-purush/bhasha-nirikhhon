import "./App.css";
import { Component } from "react";
import { Cardlist } from "./Components/Card-List/card-list.component";
import { SearchBox } from "./Components/search-box/search-box.component";

class App extends Component {
  constructor() {
    super();
    this.state = {
      monsters: [],
      searchField: "",
    };
  }
  componentDidMount() {
    fetch("http://jsonplaceholder.typicode.com/users")
      .then((response) => response.json())
      .then((users) => this.setState({ monsters: users }));
  }

  render() {
    const { monsters, searchField } = this.state;
    const filteredMonsters = monsters.filter((monsters) =>
      monsters.name.toLowerCase().includes(searchField.toLocaleLowerCase())
    );
    return (
      <div className="App">
        <SearchBox
          placeholder="Search Monsters"
          handleChange={(e) => this.setState({ searchField: e.target.value })}
        />
        <Cardlist>
          {filteredMonsters.map((monsters) => (
            <h1 key={monsters.id}>{monsters.name} </h1>
          ))}
        </Cardlist>
      </div>
    );
  }
}
export default App;

// map, reduce, shift, filter