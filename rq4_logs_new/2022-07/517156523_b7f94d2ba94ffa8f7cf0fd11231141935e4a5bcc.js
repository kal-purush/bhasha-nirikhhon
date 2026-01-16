import logo from './logo.svg';
import { useEffect, useState } from 'react'
import "./components/App.css";
import MainNav from './components/MainNav';
import SecondaryNav from './components/SecondaryNav';
import SearchPage from './components/SearchPage';
import { Route, Routes } from 'react-router-dom'
import WhatsNew from './components/WhatsNew';
import MostPopular from './components/MostPopular';
import SignUp from './components/SignUp';
import Favorites from './components/Favorites';
import CreatePost from './components/CreatePost';
import Login from './components/Login';
import 'antd/dist/antd.css';
import Post from './components/Post'
export const url = "http://localhost:3002";

function App() {
  const [logged, setLogged] = useState(false)
  const [manager, setManager] = useState(false)
  const isLogged = async () => {
    let res = await fetch(url + "/users/is-logged", { method: "GET", credentials: "include", })
    //console.log(await res.json())
    setLogged((await res.json()).isLogged)
  }
  useEffect(() => { isLogged() }, [])

  return (
    <div className="App" dir="rtl">
      <nav>
        <MainNav logged={logged} setLogged={setLogged} setManager={setManager}/>
        <SecondaryNav logged={logged} />
      </nav>
      <Routes>
        <Route path="/whats-new" element={<WhatsNew />} />
        <Route path="/search" element={<SearchPage />} />
        <Route path="/most-popular" element={<MostPopular />} />
        <Route path="/sign-up" element={<SignUp manager={manager} setLogged={setLogged} />} />
        <Route path="/posts/:postId" element={<Post manager={manager} logged={logged} />} />
        <Route path="/create-post" element={<CreatePost />} />
        <Route path="/log-in" element={<Login setManager={setManager} setLogged={setLogged} />} />
        <Route path="/favorites" element={<Favorites />} />
      </Routes>
    </div>
  );
}

export default App;