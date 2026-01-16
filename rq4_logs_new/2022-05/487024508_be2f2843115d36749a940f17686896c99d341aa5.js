import { useEffect } from 'react';
import { Navigate } from 'react-router-dom';
import './App.css';

function App() {
  const handleClick = () => {
    window.location.href = '/login';
  }

  if(localStorage.getItem('token') === null) {
    return <Navigate to="/login" replace />
  }
  
  return (
    <div className="App">
      <button onClick={handleClick}>Login</button>
    </div>
  );
}

export default App;