import './App.css';
import Header from './components/header/Header';
import Card from './components/cards/Card';
function App() {
  const name = 'test'
  const newname = name.toUpperCase()
  return (
    

    <div className="App">
        <Header url="https://mj-gallery.com/f20e3d4f-5cf7-4009-b1ce-e034588ab490/grid_0.png"/>        
        <Card url="https://mj-gallery.com/6db24f88-7735-4b9d-8bbf-85c16e1aeeb4/grid_0.png"/>
        <Card url="https://mj-gallery.com/d87bf540-3bdc-4224-b75a-cfb41893b01b/grid_0.png"/>        
      <header className="App-header">
        <h1>{name}</h1>      
        <p>{newname}</p>
        
      </header>
    </div>
  );
}

export default App;