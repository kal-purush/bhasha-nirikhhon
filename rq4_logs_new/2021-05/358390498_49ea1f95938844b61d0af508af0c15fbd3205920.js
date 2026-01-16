import Chat from './Chat';
import './App.css';
import { useDispatch } from 'react-redux'
import { useParams } from 'react-router-dom'
import {CHANGE_ROOM_NAME } from "./constants/action-types";
import axios from 'axios';
import { useEffect } from 'react';
import { WebSocketProvider } from './WebSocket';

let endpoint = "http://localhost:8080";

export const App = (props) => {

  let { room } = useParams();
  const dispatch = useDispatch()
  dispatch({type:CHANGE_ROOM_NAME, payload:room})

  useEffect(() => {
    // Call room join API
    axios.get(endpoint+""+window.location.pathname)
  })
  
  return (
    <WebSocketProvider>
      <div className="App">
        <Chat />
      </div>
    </WebSocketProvider>
  );
}

export default App;