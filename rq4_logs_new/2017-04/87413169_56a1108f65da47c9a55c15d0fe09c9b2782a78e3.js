import React, { Component } from 'react';
import data from './data/data.js'
import './App.css';
import List from './List';
import Search from './Search';

class App extends Component {

  constructor(props) {
    super(props);
    this.state = {
      filterText: ''
    }
  }

  filterUpdate(value) {
    this.setState({
      filterText: value
    })
  }

  render() {
    return (
      <div className="App">
        <Search
          filterText={this.state.filterText}
          filterUpdate={this.filterUpdate.bind(this)}
        />
        <List
          filterText={this.state.filterText}
          data={data}
         />
      </div>
    );
  }
}

export default App;