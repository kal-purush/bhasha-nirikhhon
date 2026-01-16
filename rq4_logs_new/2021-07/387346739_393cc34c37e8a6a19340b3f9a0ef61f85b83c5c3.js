//import logo from './logo.svg';
import { Component } from 'react';
import React from 'react';
import './App.css';
import './Dashboard/Dashboard.css';
import Signup from './Signup/Signup';
import Signin from './Signup/Signin';
import Navbar from './Navbar/Navbar';
import Input from './Dashboard/Input';
import Post from './Dashboard/Post';

class  App extends Component{
  state = {
    posts:[
      { user:"Fakebook Team" , message:"Thank You for using fakebbok , now You can share your posts here" , like:0 , dislike:0}
    ],
    EditMode : false
  }
  postAddHandler = () => {
    const postTemp =this.state.posts;
    console.log(postTemp);
    postTemp.splice(0,0,{user:"Anonymous user",message:document.getElementById("inpbox").value,like:0,dislike:0});
    document.getElementById("inpbox").value="";
    document.getElementById("inpbtn").innerHTML="Share";
    alert("Your message posted successfully");
    this.setState({posts:postTemp})
  }

  likeIncrease = (postIndex) => {
    const postTemp=this.state.posts;
    postTemp[postIndex].like +=1 ;
    console.log("like increase like is = "+ postTemp[postIndex].like);
    this.setState({posts:postTemp})
  }

  dislikeIncrease = (postIndex) => {
    const postTemp=this.state.posts;
    postTemp[postIndex].dislike +=1 ;
    console.log("dislike increase dislike is = "+ postTemp[postIndex].dislike);
    this.setState({posts:postTemp})
  }
  postDeleteHandler = (postIndex) => {
    const postTemp=this.state.posts;
    postTemp.splice(postIndex,1);
    console.log("post deleted number =  "+ postIndex);
    this.setState({posts:postTemp})
    alert("Your post has been deleted successfully");
  }
  postEditHandler = (postIndex) => {
    const postTemp=this.state.posts;
    document.getElementById("inpbox").value=postTemp[postIndex].message;
    postTemp.splice(postIndex,1);
    document.getElementById("inpbtn").innerHTML="Edit";
    alert("You can edit the post now . Plese correct your post in input box and then click edit");
    console.log("post edited number =  "+ postIndex);
    this.setState({posts:postTemp})
  }
  render(){
   let allposts=(
      <div>
        {this.state.posts.map(
          (post , index)=>{
            return <Post
            user ={post.user} 
            like={post.like}
            dislike={post.dislike}
            likefunc={()=>this.likeIncrease(index)}
            dislikefunc={()=>this.dislikeIncrease(index)}
            delfunc={()=>this.postDeleteHandler(index)}
            editfunc={()=>this.postEditHandler(index)}>{post.message}</Post>
          }
        )}
      </div>
    );

    return (
      <div className="App">
        <Navbar/>
        <Signup/>
        <Signin/>
        <br/><br/><br/><br/>
        <h1>FAKEBOOK</h1>
        <div className="Dashboard">
            <Input funct={this.postAddHandler }/>
            {allposts}
        </div>
      
     </div>
  );
  }
}

export default App;
