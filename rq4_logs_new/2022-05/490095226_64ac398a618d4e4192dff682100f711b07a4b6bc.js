import logo from './logo.svg';
import './App.css';
// import HelloWold from './components/HelloWorld';
// import SayMyName from './components/SayMyName';
// import Pessoa from './components/Pessoa';
// import List from './components/List';

import Evento from './components/Evento';
import Form from './components/Form';

function App() {
  return (
    <div className="App">
      <header className="App-header">
        <img src={logo} className="App-logo" alt="logo" />
        <h1>Testando eventos</h1>
        <Evento number={1}/>
        <Evento number={13}/>

        <Form />
      </header>      
    </div>
  );
}

export default App;