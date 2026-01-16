import React, {useCallback} from 'react';
import {addTodo, changeTodo} from './reducer/toDo/actions';
import './App.css';
import {useDispatch, useSelector} from 'react-redux';
import List from './components/List';
import Button from './components/Button';
import AllDone from './components/AllDone';

function App() {
  const dispatch = useDispatch();
  const list = useSelector(state => state.todo.todo);

  const addPoint = useCallback(() => {
    dispatch(addTodo(list))
  }, [dispatch, list]);

  const updatePoint =  useCallback((point) => {
    dispatch(changeTodo(point))
  }, [dispatch, list]);

  return (
    <div className="App">
      <div className="header">
        <div className="header-title">My check list</div>
        <div className="header-btn">
          <Button
            className="btn-header"
            onClick={addPoint}>Add point
          </Button>
        </div>
      </div>
      <div>
        <div className="all-done">
          <AllDone visible={list.length && list.filter(p => p.status).length === list.length}/>
        </div>
      </div>
      <List
        data={list}
        updatePoint={updatePoint}/>
    </div>
  );
}

export default App;