import React, { Component } from 'react';

import Header from '../header';
import RandomPlanet from '../random-planet';
import { PeoplePage, PlanetsPage, StarshipsPage } from "../pages";
import { SwapiProvider } from "../swapi-service-context";
import { BrowserRouter as Router, Route, Switch } from "react-router-dom";

import './app.css';
import SwapiService from '../../services/swapi-service';

export default class App extends Component {
  constructor() {
    super();
    this.state = {
      selectedItemId: null
    }
  }

  componentWillMount() {
    this.swapi = new SwapiService();
  }

  onItemSelected = (id) => {
    this.setState({
      selectedItemId: id
    })
  }

  render() {
    return (
      <SwapiProvider value={this.swapi}>
        <Router>
          <div>
            <Header />
            <RandomPlanet />
            
            <Switch>
              <Route path='/' render={() => <h2 className="text-center">Star Wars DataBase</h2>} exact />
              <Route path='/people/:id?' component={PeoplePage} exact />
              <Route path='/planets/:id?' component={PlanetsPage} />
              <Route path='/starships' component={StarshipsPage} />
              <Route render={() => <h2>Page not found</h2>}/>
            </Switch>
          </div>
        </Router>
      </SwapiProvider>
    );
  }
}