 
import React from 'react';
import CsvReader from './CsvReader'
import './App.css'

function App() {
  return (
    <div className="App">
      <header className="App-header">
        {/* Render CSV Reader */}
        <CsvReader />
      </header>
    </div>
  );
}
 
export default App;