import logo from './logo.svg';
import './App.css';
import imagem from "./logo.png"

function App() {
  const onClickBotao = () => {
    alert("Botão foi clicado!");
}

  return (
    <div>
       <div class="container">
        <h1>Aprenda React na Labenu!</h1>
        <div>
          <div>
            <img class="Logo" src={imagem} alt="logo"/>
          </div>
          <div>
            <a href="https://labenu.com.br">Site da Labenu</a>
          </div>
          <div>
            <button onClick={onClickBotao}> Clique aqui! </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;