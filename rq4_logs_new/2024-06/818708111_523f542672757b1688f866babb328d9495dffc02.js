import './App.css';
import Security from './security/security';
import Register from './register/register';
import Created from './created-done/created';
import Login from './login/login';

function App() {
  return (
    <div>      
      <Register />
      <br/>
      <Security />
      <br/>
      <Created />
      <br/>
      <Login />
    </div>
  );
}

export default App;