import React from 'react'
import './Password.css'
import UserHeader from '../components/UserHeader'
import SideNav from './SideNav'
import MobileNavbar from '../components/MobileNavbar/MobileNavbar'
const Password = () => {
  return (
    <div>
        <UserHeader/>
        <div className='passwordContainer'>
        <div className="usernav">
          <SideNav />
        </div>
        <div className='passwordHolder'><div style={{marginLeft:"0px"}}><MobileNavbar /></div>
<div className='passChange'>
<div><label>Current Password:</label><br/>
    <input /></div>
    <div>
    <label>New Password:</label><br/>
    <input />
    </div>
    <div>
    <label>Conform Password:</label><br/>
    <input />
    </div>
    <div className='passButton'> <button>SAVE</button></div>
</div>
        </div>
        </div>
    </div>
  )
}

export default Password