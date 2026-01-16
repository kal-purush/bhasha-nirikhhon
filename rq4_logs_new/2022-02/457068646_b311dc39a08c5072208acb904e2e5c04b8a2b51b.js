import React from 'react';
import ItemListContainer from './componentes/ItemListContainer';
import { BrowserRouter, Routes, Route } from 'react-router-dom';
import Home from './componentes/Home';
import NavBar from './componentes/NavBar';
import Cart from './componentes/Cart';
import 'bootstrap/dist/css/bootstrap.min.css';
import ItemDetailContainer from './componentes/ItemDetailContainer';



export default function App() {
  
  console.log(process.env);
  
  return (

    <BrowserRouter>
      <NavBar />

      <Routes>

        <Route path="/" index element={<Home />} />
        
        
        <Route path="/ItemListContainer" element={<ItemListContainer />} />
        <Route path="/ItemListContainer/:catId" element={<ItemListContainer />} />
        <Route path="/ItemDetailContainer/:itemId" element={<ItemDetailContainer/>}/>
        <Route path="/Cart" element={<Cart/>}/>
    </Routes> 
    
    </BrowserRouter >
  );
}
