import logo from './logo.svg';
import './App.css';
// import React from 'react'
import {useEffect, useState} from "react"
import axios from 'axios'
import Effect from "./Components/Effect"
import usericon from"./images/userr.png"
import usercart from"./images/cart.png"
import {Routes, Route, useNavigate , Router} from "react-router-dom"
import {Link} from "react-router-dom"
import Mp from "./Components/Mp"
import Home from "./Components/Home"
import Each from "./Components/Each"
import User from "./Components/User"
import AddGood from "./Components/AddGood"
import UserLogin from "./Components/UserLogin"
import Contact from "./Components/Contact"
import Cart from "./Components/Cart"
import Navbar from"./Components/Navbar"
import {increment} from './actions/increase'
import {useSelector, useDispatch} from "react-redux"
import Footer from "./Components/Footer"
import Checkout from "./Components/Checkout"
import { PaystackButton } from 'react-paystack'

// const log= require('logger-with-time')
// log()
function App() {
  const url="http://odiduo.herokuapp.com/all"
  const username = useSelector(state=>state.name)
  const navigate = useNavigate()
  const dispatch = useDispatch()
  const name = useSelector(state=> state.name)
  const count = useSelector(state=>state.count)
  return (
    <>    
          <Routes>
          <Route path="/" element={<Home/>}/>        
          <Route path="/mp" element={<Mp/>}/>
          <Route path="/home" element={<Home/>}/>
          <Route path="/user" element={<User/>}/>
          <Route path="/userlogin" element={<UserLogin/>}/>        
          <Route path="/item/*" element={<Each/>}/>
          <Route path="/addgood" element={<AddGood/>}/>
          <Route path="/contact" element={<Contact/>}/>
          <Route path="/cart" element={<Cart/>}/>
          <Route path="/checkout" element={<Checkout/>}/>             
          </Routes>
    </>
      );
    }
    export default App;
    
