import React from 'react';
import Header from './components/Header';
import Section from './components/Section';

class App extends React.Component{
  render() {
    return(
      <div>
        <Header/>
        <Section/>
      </div>
    );
  }
}

export default App;