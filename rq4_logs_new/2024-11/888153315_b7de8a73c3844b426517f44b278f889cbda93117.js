import React from "react";
import { BrowserRouter as Router, Routes, Route, Link } from "react-router-dom";
import './App.css';
import Home from "./pages/Home";
import Cadastro from "./pages/Cadastro";
import ProdutoDetalhado from "./pages/Produto";
import Carrinho from "./pages/Carrinho";
import Confirmacao from "./pages/Confirmacao";

function App() {
  return (
    <div className="App">
      <Router>
      <header className="bg-gray-800 text-white py-4 shadow">
      <div className="container mx-auto flex justify-between items-center px-6">
        <h1 className="text-2xl font-bold">Loja</h1>
        <nav className="flex gap-6">
          <Link className="hover:text-gray-600 text-white no-underline" to="/">Inicio</Link>
          <Link className="hover:text-gray-600 text-white no-underline" to="/cadastro">Cadastro</Link>
          <Link className="hover:text-gray-600 text-white no-underline" to="/carrinho">Carrinho</Link>
        </nav>
      </div>
    </header>

      <Routes>
        <Route path="/" element={<Home />} />
        <Route path="/cadastro" element={<Cadastro />} />
        <Route path="/produto/:id" element={<ProdutoDetalhado />} /> 
        <Route path="/carrinho" element={<Carrinho />} /> 
        <Route path="/confirmacao" element={<Confirmacao />} /> 
      </Routes>
    </Router>
    </div>
  );
}

export default App;