import { Route } from 'react-router-dom';
import './App.css';
import './search.css';
import Home from './Pages/Home';
import Search from './Pages/Search';

function App() {
  return (
    <div className="App">
      <Route path='/' exact component={Home} />
      <Route path='/search' component={Search} />
    </div>
  );
}

export default App;