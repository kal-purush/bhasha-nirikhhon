import styles from './App.css';
import TodoList from './Components/TodoList';

// Main app component that shows a brand bar and the todo list
function App() {
  return (
    <div className="App">
      <nav className="navbar navbar-expand-sm bg-primary">
        <div className="container-fluid">
          <a className="navbar-brand" href="#">TODO App</a>
        </div>
      </nav>
      <TodoList/>
    </div>
  );
}

export default App;