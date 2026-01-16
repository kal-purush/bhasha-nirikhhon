import React,{Component} from 'react';
import './App.css';
import {BrowserRouter , Route} from "react-router-dom";
import { Home } from "./components/Home";
import {AboutUs} from "./components/AboutUs";
import {Register} from './components/Register';
import {Login} from "./components/Login";
import {Mainpage} from './components/Mainpage';
import { Careers } from './components/Careers';
import { ContactUs } from './components/ContactUs';
import { Blog } from './components/Blog';
import { UserHome } from './components/UserHome';
import {Apply} from './components/Apply';
import {Playlist} from './components/Playlist';
class App extends Component {
  //landing page to home and main page
  render() {
    return (
      // mention all your routes Header, this is like the index
      <BrowserRouter>
        <div>
        {/* <Route path="/" component={Root}/> */}
        {/* //exact ensure it is called only once */}
        <Route path="/" component={Home} exact/> 
        <Route path="/aboutus" component={AboutUs}/>
        <Route path="/register" component={Register} />
        <Route path="/careers" component={Careers} />
        <Route path="/login" component={Login} />
        <Route path="/contactus" component={ContactUs} />
        <Route path="/mainpage" component={Mainpage}/>
        <Route path="/blog" component={Blog}/>
        <Route path="/UserHome" component={UserHome}/>
        <Route path="/Playlist" component={Playlist}/>
        <Route path="/Apply" component={Apply}/>
        {/* route is bascially the way the url in browser should look, even
        if you type localoat:8080/About, the page will display
        Here the path is what will get appended to localhost and component is what should be rendered*/}
        </div>
      </BrowserRouter>
    );
  }
}

export default App;