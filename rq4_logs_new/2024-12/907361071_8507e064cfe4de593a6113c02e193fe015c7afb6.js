import logo from './logo.svg';
import './App.css';
import ToDoCont from "./Components/ToDoProviderApp/ToDoCont.js"
import InputForm from './Components/ToDoProviderApp/InputForm.js';

function App() {
  return (
    <div  className = "todo-form" style={{marginLeft: "30px", marginTop: "30px", }}>
      <h1 className='todo-title'>ToDo Form</h1>
        <InputForm></InputForm>
        <ToDoCont></ToDoCont>
    </div>
  );
}


export default App;