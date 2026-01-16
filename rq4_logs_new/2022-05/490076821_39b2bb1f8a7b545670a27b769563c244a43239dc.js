import React from 'react';
import { AppBar, Toolbar, IconButton, Badge, Typography } from '@mui/material'; //MenuItem, Menu, 
import { AddShoppingCart } from '@mui/icons-material';
import { Link } from '@mui/material';
import logo from '../../assets/kindpng_212280.png';



const Navbar = ({ totalItems }) => {
  return (
    <>
      <AppBar position="fixed" color="inherit">
        <Toolbar>
          <Typography component={Link} to="/" variant="h6" color="inherit">
            <img src={logo} alt="E-commerce5" height="25px"/>
            E-Commerce5
          </Typography>
          {/* <div  /> */} 
          <div>
          <Link to="/cart">go to cart</Link>
            <IconButton component={Link} to='/cart' aria-label="Show cart items" color="inherit">
              <Badge badgeContent={totalItems} color="secondary">
                <AddShoppingCart />
              </Badge>

            </IconButton>
          </div>
        </Toolbar>
      </AppBar>
    </>
  )
}

export default Navbar