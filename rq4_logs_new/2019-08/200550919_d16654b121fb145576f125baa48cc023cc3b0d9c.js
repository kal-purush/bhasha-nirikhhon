import React from 'react';
import 'bootstrap/dist/css/bootstrap.min.css'
import './App.css'
import { BrowserRouter as Router, Route } from 'react-router-dom'
import Navbar from './components/navbar'
import ListaPlacas from './components/listaPlacas'
import CrearPlaca from './components/crearPlaca'
import CrearUsuario from './components/crearUsuario'
import { Container } from 'react-bootstrap';


function App() {
  return (
    <Router>
      <Navbar />
      <Container className="p-4">
      <Route path="/" exact component={ListaPlacas} />
      <Route path="/crearPlaca" component={CrearPlaca} />
      <Route path="/editar/:id" component={CrearPlaca} />
      <Route path="/crearUsuario" component={CrearUsuario} />
      </Container>
    </Router>
  );
}

export default App;