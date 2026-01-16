import "./App.css";
import {useDispatch, useSelector} from "react-redux";
import {incCounter,decCounter,resetCounter} from "./reducers/counter/counter";
import {addTodos,removeTodo} from "./reducers/todo/todo"
import axios from "axios";




function App() {



  const dispatch = useDispatch();
  const state = useSelector((state) => {

    return {
      counter: state.counter.counter,
      todos: state.todos.todos,
    };
  });



  const inc = () => {
    dispatch(incCounter(1));
  };
  const dec = () => {
    dispatch(decCounter(1));
  };
  const reset = () => {
    dispatch(resetCounter());
  };


  function onAddClick(){
    axios.get("https://jsonplaceholder.typicode.com/todos/")
      .then((res) => {
        let _arr = [...state.todos]
        let arr = state.todos.length;
        let updateTodo = _arr.concat(res.data[arr])
        dispatch(addTodos(updateTodo));

      })
 }

 function onDeleteClick (id){
  let todoarray = [...state.todos];
  console.log(todoarray);
  todoarray[id-1]=null;
  console.log(todoarray);
  dispatch(removeTodo(todoarray))
 }

  return (


      <div className="App">
        <h2>{state.counter}</h2>
        <button className='btn' onClick={inc}>Increase</button>
        <button className='btn' onClick={dec}>Decrease</button>
        <button className='btn' onClick={reset}>Reset</button>

        <hr/>


      <br/><button className='btn' onClick={() => {onAddClick()}}>Add Todo</button>

          <ul style={{ listStyleType: "none" }}>
            {state.todos.map((element) => {
              if (element) {
                  return (<li><br/>{element.title}<br/><button className='btn1' onClick={() => {onDeleteClick(element.id)}}>Delete</button></li>)
              }})
            }

          </ul>
          </div>
          );
}

export default App


  