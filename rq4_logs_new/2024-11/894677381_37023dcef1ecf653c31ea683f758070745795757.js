import React, { useState, useEffect } from 'react';
import './App.css';

const App = () => {
  const [alunos, setAlunos] = useState([]);
  const [novoAluno, setNovoAluno] = useState({ nome: '', idade: '', curso: '' }); // Estado para o novo aluno a ser adicionado na lista

  const API_URL = 'http://localhost:3000';

  // Função para buscar alunos
  const fetchAlunos = async () => {
    try {
      const response = await fetch(`${API_URL}/api/alunos`);
      const data = await response.json();
      setAlunos(data);
    } catch (err) {
      console.error('Erro ao buscar alunos:', err);
    }
  };


  useEffect(() => {
    fetchAlunos();
  }, []); // O array vazio garante que o useEffect execute apenas uma vez

  // Função para adicionar um aluno
  const adicionarAluno = async () => {
    try {
      const response = await fetch(`${API_URL}/api/alunos`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(novoAluno),
      });
      if (response.ok) {
        fetchAlunos(); // Atualiza a lista de alunos após adicionar
        setNovoAluno({ nome: '', idade: '', curso: '' });
      } else {
        console.error('Erro ao adicionar aluno');
      }
    } catch (err) {
      console.error('Erro ao adicionar aluno:', err);
    }
  };

  return (
    <div>
      <h1>Gerenciamento de Alunos</h1>

      <div className="principal">
        <h2>Adicionar Aluno</h2>
        <input
          type="text"
          placeholder="Nome"
          value={novoAluno.nome}
          onChange={(e) => setNovoAluno({ ...novoAluno, nome: e.target.value })}
        />
        <input
          type="number"
          placeholder="Idade"
          value={novoAluno.idade}
          onChange={(e) => setNovoAluno({ ...novoAluno, idade: e.target.value })}
        />
        <input
          type="text"
          placeholder="Curso"
          value={novoAluno.curso}
          onChange={(e) => setNovoAluno({ ...novoAluno, curso: e.target.value })}
        />
        <button onClick={adicionarAluno}>Adicionar</button>
      </div>

      <div>
        <h2>Lista de Alunos</h2>
        <ul>
          {alunos.map((aluno) => (
            <li key={aluno.id}>
              {aluno.nome} - {aluno.idade} anos - {aluno.curso}
            </li>
          ))}
        </ul>
      </div>
    </div>
  );
};

export default App;