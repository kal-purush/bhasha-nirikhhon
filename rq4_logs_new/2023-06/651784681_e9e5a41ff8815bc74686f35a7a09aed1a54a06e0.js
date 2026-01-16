import React from 'react'
import Navbar from './components/header/Navbar'
import Newnav from './components/newnavbar/Newnav'
import Maincomp from './components/home/Maincomp'
import Footer from './components/footer/Footer'
import Sign_in from './components/signup_signin/Sign_in'
import Sign_up from './components/signup_signin/Sign_up'

import { Routes,Route } from 'react-router-dom'

const App = () => {
  return (
    <>
    <Navbar/>
    <Newnav/>
    <Routes>
      <Route path='/' element={<Maincomp/>}/>
      <Route path='/login' element={<Sign_in/>}/>
      <Route path='/register' element={<Sign_up/>}/>
    </Routes>
    <Footer/>
    </>
  )
}

export default App