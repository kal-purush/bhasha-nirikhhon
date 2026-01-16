import React from 'react';
import './App.css';
import { BrowserRouter, Route, Switch } from 'react-router-dom';
import ResponsiveContainer from '../Container/ResponsiveContainer';



function App() {
  return (
    <BrowserRouter>
      <ResponsiveContainer>
        <div style={{marginTop: '130em'}}>We Help Companies and Companions</div>
        <div>We Help Companies and Companions</div>
        <div>We Help Companies and Companions</div>
        <div>We Help Companies and Companions</div>
        <div>We Help Companies and Companions</div>
        <div>We Help Companies and Companions</div>
        <Switch>
          <Route>
          </Route>
        </Switch>
      </ResponsiveContainer>
    </BrowserRouter>
  );
}

export default App;