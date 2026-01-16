import React from 'react'

import './App.css';

import Header from "../Header/Header.jsx";
import Inicio from '../Inicio/Inicio.jsx';
import Cursos from '../Cursos/Cursos.jsx';


import { Route, Switch } from 'react-router'
function App() {
  return (
    <div>
    
      <Header />

      <main>
        <Switch>

          <Route exact path = "/" render = {(props) => <Inicio/>}> </Route>

        </Switch>
      </main>

    </div>
  );
}

export default App;
