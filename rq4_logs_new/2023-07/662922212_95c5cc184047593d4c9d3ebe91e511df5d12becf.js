import './App.css';
import Header from "./MyComponents/header";
import Blogs from "./MyComponents/blogs";
import Footer from "./MyComponents/footer"
import React, {useState,useEffect} from 'react';
import AddBlog from './MyComponents/AddBlog';
import AddComment from './MyComponents/AddComment';
import Comments from './MyComponents/Comments';
import About from './MyComponents/About';
import Contact from './MyComponents/contact';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
function App() {
  let initComment;
  if(localStorage.getItem("comments")===null){
     initComment=[];
  }
  else{
    initComment=JSON.parse(localStorage.getItem("comments"));
  }
  const addComment = (name,com,email, btitle)=>{
    let csno;
    if(comments.length===0){
      csno=0;
    }
    else{
       csno=comments[comments.length-1].csno+1;
    }
    const myComment={
      csno:csno,
      btitle:btitle,
      com:com,
      name:name,
      email: email
    }
    setComments([...comments, myComment])
    }
    const [comments, setComments]=useState([...initComment
    
    ]);
    useEffect(()=>{
      localStorage.setItem("comments",JSON.stringify(comments));
      },[comments])
  let initBlog;
  if(localStorage.getItem("blogs")===null){
    initBlog=[{
      sno:1,
      title:"Wild Animals",
      desc: "Travelling broadens the mind. We can learn about new places and people and, in doing so, we learn about ourselves. And yet, its essential that we do not allow...",
       image:"https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcSx_5oPunvhgC84Q13lOi-vEFhCb8Rsf6dZWsdznQteaA&usqp=CAU&ec=48665699" ,
       submissionTime: ""
    },
    {
      sno:2,
      title:"Birds",
      desc:"birds are ijmportant in our life we should save them",
      image:"https://th.bing.com/th?id=ALSTU353157067FBF76B5E5C696EB748968022E0519A1DC1668EC4E3DB7FA23BE4BE1&w=472&h=281&rs=2&o=6&oif=webp&dpr=1.3&pid=SANGAM",
      submissionTime: ""
    }];
  }
  else{
    initBlog=JSON.parse(localStorage.getItem("blogs"));
  }
  function onDelete(blog) {
    setBlogs(blogs.filter((e) => {
      return e !== blog;
      
    }));
    localStorage.setItem("blogs",JSON.stringify(blogs));
  }
  const addBlog = (title,desc,link, currentTime)=>{
    let sno;
    if(blogs.length===0){
      sno=0;
    }
    else{
  sno=blogs[blogs.length-1].sno+1;
    }
  const myBlog={
    sno:sno,
    title:title,
    desc:desc,
    image:link,
    submissionTime: currentTime
  }
  setBlogs([...blogs, myBlog])
 
  
  
  }
  const [blogs, setBlogs]=useState([...initBlog
    
  ]);
  useEffect(()=>{
    localStorage.setItem("blogs",JSON.stringify(blogs));
    },[blogs])
  return (
   <>
   <Router>
   <Header title ="My blogs list"/>
   <Routes>
        <Route exact path="/" element={<><AddBlog addBlog={addBlog} > addBlog</AddBlog>
        
    <Blogs blogs={blogs} onDelete={onDelete}/></>} />
    <Route exact path="/comment" element={<AddComment addComment={addComment} > addComment</AddComment>}/>
    <Route exact path='/comments' element={<Comments comments={comments} /> }/>
        <Route exact path="/about" element={<About />} />
        <Route exact path="/contact" element={<Contact />} />
        
        
    </Routes>
    <Footer/>
   </Router>
   </>
  );
}

export default App;