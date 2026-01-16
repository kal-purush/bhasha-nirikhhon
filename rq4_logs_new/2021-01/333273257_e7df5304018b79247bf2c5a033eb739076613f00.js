import React, { useState } from 'react';
import {
  BrowserRouter as Router,
  Switch,
  Route,
  Link,
} from "react-router-dom";
import './App.css';
import Addalbum from './components/Addalbum';
import AlbumDetail from './components/AlbumDetail';
import AlbumList from './components/AlbumList';
import externalalbums from './albums'

const sortedalbum = [...externalalbums].sort((a, b) => new Date(a.publishedDate) - new Date(b.publishedDate))
function App() {

  const [currentalbums, setalbums] = useState(sortedalbum);

  const handleAddalbum = (newalbum) => {
    setalbums([newalbum, ...currentalbums])
  }

  return (
    <div className="App">
      <Router>
        <div>
          <nav>
            <ul className="horizontal">
              <li>
                <Link to="/">Home</Link>
              </li>
              <li>
                <Link to="/add-album">Add new album</Link>
              </li>
              <li>
                <Link to="/albums">albums</Link>
              </li>
            </ul>
          </nav>
          <Switch>
            <Route exact path="/">
              <Home />
            </Route>
            <Route path="/add-album">
              <Addalbum onAddalbum={handleAddalbum}/>
            </Route>
            <Route path="/albums">
              <AlbumList albums={currentalbums} />
            </Route>
            <Route path={`/album/:albumId`}>
              <AlbumDetail albums={currentalbums} />
            </Route>
          </Switch>
        </div>
      </Router>
    </div>
  );
}

function Home() {
  return <h2>Welcome to the final checkpoint!</h2>;
}

export default App;