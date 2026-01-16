import './App.css';
import logo from './img/logo_jlimones.jpg'
import Head from './components/Head'
import HeadTasks from './components/bodyTask'

function App() {
  return (
    <div className="App_task">
      <Head img = {logo} />
      <HeadTasks  />
    </div>
  );
}

export default App;