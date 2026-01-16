import React from "react";
import{Routes,Route, BrowserRouter} from "react-router-dom"
import Mobiledata from "./mobiles";
import Laptopdata from "./laptops";
import Navbar from "./Navigate";
import { AppBar,Container, Stack, Typography } from "@mui/material";
import HomeIcon from '@mui/material/Icon'


import Cart from "./cart";


const App=()=>{
    return(
      <React.Fragment>
        <BrowserRouter>
        <div>
            <Routes>
              
            <Route path="/" element={<Home />} />
                <Route path="/mobiles" element={<Mobiledata />} />
                <Route   path="/laptops" element={<Laptopdata />} />
                <Route path="/cart" element={<Cart />} />

            </Routes>
            <div>  <AppBar position="static">
            <Container maxWidth="xl">

            <Typography
            variant="h6"
            noWrap
            component="a"
            href="#app-bar-with-responsive-menu"
            sx={{
              mr: 2,
              display: { xs: 'none', md: 'flex' },
              fontFamily: 'monospace',
              fontWeight: 700,
              letterSpacing: '.3rem',
              color: 'inherit',
              textDecoration: 'none',
            }}
          >
                <Navbar />
                </Typography>
                </Container></AppBar>
            </div>
        </div></BrowserRouter>
        <p>
          <b>
            <center>
            welcome to vinareddy 
            <div>
              Happy shopping
            </div>
</center>
          </b>
        </p>
        
        </React.Fragment>
    )
}

const Home = () => {
  
    return <div> <Stack direction="row" spacing={3}>
    <HomeIcon />
    <HomeIcon color="pink" />
   
  </Stack></div>;
};
export default App;