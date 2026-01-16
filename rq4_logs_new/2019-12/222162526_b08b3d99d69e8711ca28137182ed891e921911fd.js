import React from 'react';
import './Sidedrawer.css';

import Logo from '../../Logo/Logo';
import NavigationItems from '../NavigationItems/NavigationItems';

const sideDrawer = (props) => {
  return(
    <div className='SideDrawer'>
      <div className='sidedrawerLogo'>
        <Logo />
      </div>

      <nav>
        <NavigationItems />
      </nav>
    </div>
  );
};

export default sideDrawer;