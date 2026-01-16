import React from 'react';
import { BrowserRouter as Router, Route, Routes } from 'react-router-dom';
import Header from './componentes/Header';
import Main from './componentes/Main';
import Detalles from './componentes/Detalles';
import Footer from './componentes/Footer';
import Tierlist from './componentes/Tierlist';
import SignupForm from './componentes/SignupForm';
import LoginForm from './componentes/LoginForm';
import Favoritas from './componentes/Favoritas';
import { useEffect, useState } from 'react';


function App() {
  const [isAuthenticated, setIsAuthenticated] = useState(localStorage.getItem('isAuthenticated') === 'true');
  const [valorEmail, setValorEmail] = useState('');
  const [fromFavoritas, setFromFavoritas] = useState(false);

  useEffect(() => {
    // Verificar si el usuario está autenticado en localStorage
    const storedIsAuthenticated = localStorage.getItem('isAuthenticated') === 'true';
    // Establecer el estado de autenticación en consecuencia
    setIsAuthenticated(storedIsAuthenticated);
  }, []);

  useEffect(() => {
    // Recupera el email del usuario desde localStorage al cargar la página
    const storedUserEmail = localStorage.getItem('userEmail');
    if (storedUserEmail) {
      setValorEmail(storedUserEmail);
    }
  }, []);

  const handleLogout = () => {
    setIsAuthenticated(false);
    localStorage.removeItem('isAuthenticated');
  };
  
  return (
    <Router>
      <div>
        <Header isAuthenticated={isAuthenticated} handleLogout={handleLogout} />
        <Routes>
          <Route exact path="/" element={<Main setFromFavoritas={setFromFavoritas} />} />
          <Route path="/pelicula/:id" element={<Detalles isAuthenticated={isAuthenticated} valorEmail={valorEmail} fromFavoritas={fromFavoritas} />} />
          <Route path="/tierlist" element={<Tierlist />} />
          <Route path="/crearcuenta" element={<SignupForm />} />
          <Route path="/sesion" element={<LoginForm isAuthenticated={isAuthenticated} setIsAuthenticated={setIsAuthenticated} valorEmail={valorEmail} setValorEmail={setValorEmail} />} />
          <Route path='/favoritas' element={<Favoritas valorEmail={valorEmail} setFromFavoritas={setFromFavoritas} />} />
        </Routes>
        <Footer />
      </div>
    </Router>
  );
}

export default App;