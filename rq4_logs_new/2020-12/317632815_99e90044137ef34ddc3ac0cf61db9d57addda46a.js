import './App.css';
import { BrowserRouter as Router, Switch, Route } from 'react-router-dom'
import Home from './components/home';
import Login from './components/login';
import withAuth from './AuthWrapper';

function App() {
  return (
    <Router>
      <Switch>
        <Route path='/login' component={Login} />
        <Route path='/' component={withAuth(Home)} />
      </Switch>
    </Router>
  );
}

export default App;