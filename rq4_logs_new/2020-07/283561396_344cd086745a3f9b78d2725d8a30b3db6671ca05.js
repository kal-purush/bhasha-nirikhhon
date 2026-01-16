import React from 'react';
import './App.css';
import  Yalantis  from '../../util/Yalantis';
import MonthList from '../MountList/MonthList'
import UserList from '../UserList/UserList'

class App extends React.Component {
  constructor(props){
    super(props);
    this.state = {
      monthId:null,
      users:[],
      isFetching: true,
      months:[
      {id:'1',name:"January"},
      {id:'2',name:"February"},
      {id:'3',name:"March"},
      {id:'4',name:"April"},
      {id:'5',name:"May"},
      {id:'6',name:"June"},
      {id:'7',name:"July"},
      {id:'8',name:"August"},
      {id:'9',name:"September"},
      {id:'10',name:"October"},
      {id:'11',name:"November"},
      {id:'12',name:"December"},]
    };
  }
  componentDidMount(){
    Yalantis.start().then(users=>{
      this.setState({users:Yalantis.result});
      if(this.state.users!==undefined)
      this.setState({isFetching: false});
    })
  }
  handleChangeMonth(id){
    this.setState({monthId:id})
  }
  render(){
    if(this.state.isFetching) {return (<h1>...Loading...</h1>)};
  return (
    <div className="App">
      <h1>Yalantis Months</h1>    
      <div className = "MonthList">
      <MonthList users={this.state.users} months={this.state.months} changeList={this.handleChangeMonth.bind(this)}/>
      </div>
      <div className ="UserList">
      <UserList users={this.state.users[this.state.monthId-1]}/>
      </div>
    </div>
  );
}
}

export default App;