import logo from './logo.svg';
import React from "react";
import {BrowserRouter as Router, Route} from "react-router-dom";
import './App.css';
import PostListPage from "./pages/PostListPage";
import PostDetailPage from "./pages/PostDetailPage";
import WritePostPage from "./pages/WritePostPage";

function App() {
  return (
    <div className="App">
        <Router>
            <Route exact path="/" component={PostListPage}></Route>
            <Route exact path="/posts/:postId" component={PostDetailPage}></Route>
            <Route exact path="/write/posts" component={WritePostPage}></Route>
        </Router>
    </div>
  );
}

export default App;