import axios from "axios";
import React, { useEffect } from "react";
import { useDispatch, useSelector } from "react-redux";
import { Routes, useLocation, useNavigate } from "react-router";
import { Route } from "react-router-dom";
import Navbar from "./commons/Navbar";
import Index from "./components/Index";
import Login from "./components/Login";
import Main from "./components/Main";
import Account from "./components/MyAccount";
import Register from "./components/Register";
import Search from "./components/Search";
import Actor from "./components/Actor";
import Movie from "./components/Movie";
import { setUser } from "./store/user";
import "./styles/index.css";

const App = () => {
  const user = useSelector((state) => state.user);
  const dispatch = useDispatch();
  const location = useLocation();
  const navigate = useNavigate();
  useEffect(() => {
    axios
      .get("http://localhost:5000/api/users/me", {
        withCredentials: true,
      })
      .then((user) => dispatch(setUser(user.data)))
      .catch((error) => console.error(error));
  }, [location, location.pathname]);

  return (
    <>
      <Navbar />
      <Routes>
        {!user.email ? (
          <>
            <Route path="/" element={<Index />} />
            <Route path="/register" element={<Register />} />
            <Route path="/login" element={<Login />} />
          </>
        ) : (
          <>
            <Route path="/main" element={<Main />} />
            <Route path="/:media_type/single/:id" element={<Movie />} />
            <Route path="/actor/single/:id" element={<Actor />} />
            <Route path="/search" element={<Search />} />
            <Route path="/account" element={<Account />} />
          </>
        )}
      </Routes>
    </>
  );
};

export default App;