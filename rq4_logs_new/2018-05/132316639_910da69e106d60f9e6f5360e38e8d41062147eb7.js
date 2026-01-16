import React, { Component } from 'react';

import './App.css';
import InputForm from './Forms';

class App extends Component {
  state = {
      cards : [
            // {name:"Goutham Peepala",
            // avatar_url:"https://avatars2.githubusercontent.com/u/17699180?v=4",
            // company:"Citrix R&D India Pvt Ltd"},
            // {name:"Sai Kumar",
            // avatar_url:"https://avatars.githubusercontent.com/u/8445?v=3",
            // company: "HCL Technologies"}
        ]
    }
  render() {
    return (
      <div className="App">
        <header >
          <img src="GitHub.jpg"/>
          <h1 className="App-title">GitHub User Cards</h1>
        </header>
        <p className="App-intro">
          Please enter the GitHub Username and fetch.
        </p>
        <InputForm userInfo={this.retrieveUserInfo} userCards={this.state.cards}/>
      </div>
    );
  }
  retrieveUserInfo = (userInfo) =>{
    console.log("The UserInfo is "+ userInfo);
    this.setState ((prevState) => ({
        cards : this.state.cards.concat(userInfo)
    }));
  }

}

export default App;