import React from 'react';
import { useSelector } from 'react-redux';

const MinhaEstante = () => {
  const { user } = useSelector((state) => state.usuario);

  return (
    <div>
      <h1>Bem-vindo, {user?.nome || 'Usuário'}!</h1>
      <h2>Minha Estante</h2>
      <ul>
        <li><a href="/doar-livro">Doar Livro</a></li>
        <li><a href="/pegar-livro">Pegar Livro</a></li>
        <li><a href="/devolver-livro">Devolver Livro</a></li>
        <li><a href="/livros-lendo">Livros que Estou Lendo</a></li>
        <li><a href="/livros-doados">Livros que Já Doei</a></li>
        <li><a href="/postar-quote">Postar uma Quote</a></li>
        <li><a href="/avaliar-livro">Avaliar um Livro</a></li>
      </ul>
    </div>
  );
};

export default MinhaEstante;