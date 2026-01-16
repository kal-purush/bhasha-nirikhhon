import React, { Component } from 'react';
import logo from './logo.svg';
import './App.css';
import Sampletwo from './sampletwo.js';
import SampleThree from './Samplethree.js';


class App extends Component {
  constructor(props){
      super(props);
      this.state = {
        name : "prathiksha",
        age : 25
      };
      this.update = this.update.bind(this); // binding the scope
  }
  shouldComponentUpdate(nextProps, nextState){
    if(nextState.name == "madhuri"){
      return false;
    }else{
      return true;
    }
  }
  update(){
    this.setState({name:"kishore"});
  }
  render() {
    return (
      <div className="App">
        <header className="App-header">
          <img src={logo} className="App-logo" alt="logo" />
          <h1 className="App-title">Welcome to React{this.state.name}</h1>
        </header>
        <p className="App-intro">
          To get started, edit <code>src/App.js</code> and save to reload.
        </p>
        <button onClick={()=>{this.update()}} >Click</button>
        <button onClick={()=>{this.setState({name:"kishore"})}} >Change the value </button>
        <Sampletwo class={this.state.name}/>
        <SampleThree />
        
      </div>
    );
  }
}



export default App;