import './App.css';
import Chat from './components/Chat';
import Signin from './components/SignIn';
import { auth } from './Firebase';
import { useAuthState } from 'react-firebase-hooks/auth';

function App() {
  const [user] = useAuthState(auth);
  return <>{user ? <Chat /> : <Signin />}</>;
}

export default App;