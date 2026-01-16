import React from 'react';
import './App.css';
import Tweets from'./Tweets'

class App extends React.Component{
 constructor(props){
   super(props);
     this.state={
         items:[],
         currentItem:{
           text:'',
           key:''
         }
       }
       //this.handleInput=this.handleInput.bind(this);
       //this.tweets=this.tweets.bind(this);
     }
  onInputChange=(e)=>{
   this.setState({
     currentItem:{
       text:e.target.value,
       key:Date.now()
     }
   })
 }
 tweets=(e)=>{
   e.preventDefault();
   const newItem=this.state.currentItem; 
   console.log(newItem);
   if(newItem.text!==""){
     const newItems=[...this.state.items,newItem];
     this.setState({
       items:newItems,
       currentItem:{
         text:'',
         date:''
       }
     })
   }
 }
  render(){
    return(
      <div className="App">
        <header>
        <form id="DisappearingTweet" onSubmit={this.tweets}>
          <textarea row="50" cols="40" placeholder="What's happening?" value={ this.state.currentItem.text}
          onChange={this.onInputChange}/>
          <button type="submit">Tweet</button>
          </form>
          </header>
          <Tweets items = {this.state.items} >
          </Tweets>
          
      </div>
    )
  }
  
}
export default App