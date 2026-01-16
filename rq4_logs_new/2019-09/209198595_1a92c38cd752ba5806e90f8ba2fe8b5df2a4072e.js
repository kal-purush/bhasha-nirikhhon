import React from 'react';
import {CardList} from './components/card-List/card-list.component'
import './App.css';
import {Search} from './components/search/search.component';

class App extends React.Component {
  constructor(){
    super();
    this.state = {
      monsters :[
      ],
      searchField : ""
    };
  } 

  componentDidMount(){
    fetch('https://jsonplaceholder.typicode.com/users').
    then(response =>response.json()).then(users =>this.
      setState({monsters:users}));
  }
  render(){
    const {monsters,searchField} = this.state;
    const filteredMonster = monsters.filter
    (monster =>monster.name.toLowerCase().includes(searchField.toLowerCase()));
    return (
  
    <div className="App">
      <h1>Monsters Era</h1>
      <Search placeholder = 'Search Monsters'
      handleChange = {c=> this.setState({
         searchField: c.target.value })
      }
       />
       <CardList sharan ={filteredMonster}> </CardList>
     
      
    </div>
  );
  }
}
export default App;