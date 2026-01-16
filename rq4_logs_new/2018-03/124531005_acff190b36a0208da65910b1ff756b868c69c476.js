import React, { Component } from 'react';
import './App.css';
import Header from './Header';
import Footer from './Footer';
import FlexParent from './FlexParent';

class App extends Component {
  constructor(props) {
    super(props);
    this.state = {isToggleOn: true};
    this.handleClick = this.handleClick.bind(this);
  }
  handleClick() {
    this.setState(prevState => ({
      isToggleOn: !prevState.isToggleOn
    }));
  }
  render() {
    const buttonStyle = {
      height : 50,
      padding : 10
    }
    return (
      <div>
        <Header/>       
        <i class="material-icons buttonSty" onClick={this.handleClick}>{this.state.isToggleOn ? 'view_module' : 'view_column'}</i>
        <h6>{this.state.isToggleOn ? 'row' : 'column'}</h6>
        <FlexParent cName={this.state.isToggleOn ? 'row' : 'column'} />
        <Footer/>
      </div>
    );
  }
}

export default App;