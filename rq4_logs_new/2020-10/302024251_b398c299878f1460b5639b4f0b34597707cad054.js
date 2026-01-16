import React, {Component} from 'react';
import {BrowserRouter as Router, Route, Switch} from "react-router-dom";

import ErrorBoudry from "../ErrorBoudry/ErrorBoudry";
import ErrorButton from "../ErrorButton/ErrorButton";

import SwapiService from "../../servicies/SwapiService";
import DummySwapiService from "../../servicies/DummySwapiService";
import {SwapiServiceProvider} from "../SwapiServiceConext/SwapiServiceConext";

import Header from "../Header/Header";
import RandomPlanets from "../RandomPlanets/RandomPlanets";
import PeoplePage from "../Pages/PeoplePage";
import PlanetPage from "../Pages/PlanetPage";
import StarshipsPage from "../Pages/StarshipsPage";

import './App.css'
import StarshipDetails from "../SwComponents/StarshipDetails";
import LoginPage from "../Pages/LoginPage";
import SecretPage from "../Pages/SecretPage";


export default class App extends Component {
    state = {
        showRandomPlanet: true,
        swapiService: new SwapiService(),
        isLoggedIn: false
    };

    onLogin = () => {
        this.setState({isLoggedIn: true});
    };

    onServiceChange = () => {
        // позволяет обновлять сервисы данных при их изменении
        // передаем из state
        this.setState(({swapiService}) => {
            const Service = swapiService instanceof SwapiService ?
                DummySwapiService : SwapiService;

            return {
                swapiService: new Service()
            }
        });

    };

    toggleRandomPlanet = () => {
        this.setState((state) => {
            return {showRandomPlanet: !state.showRandomPlanet}
        });
    };

    render() {

        const planet = this.state.showRandomPlanet ? <RandomPlanets/> : null;
        const {isLoggedIn} = this.state;

        return (
            <ErrorBoudry>
                <SwapiServiceProvider value={this.state.swapiService}>
                    <Router>
                        <div className='stardb-app'>
                            <Header onServiceChange={this.onServiceChange}/>
                            {planet}

                            <div className="row mb2 button-row ">
                                <button
                                    className="toggle-planet btn btn-warning btn-lg"
                                    onClick={this.toggleRandomPlanet}>
                                    Toggle Random Planet
                                </button>
                                <ErrorButton/>
                            </div>
                            <Switch>
                                <Route path='/' exact render={() => <h2>Welcom to StarDB</h2>}/>
                                <Route path='/people/:id?'
                                       component={PeoplePage}/> {/*подходит для people и people/id*/}
                                <Route path='/planets' component={PlanetPage}/>
                                <Route path='/starships' exact component={StarshipsPage}/>

                                <Route path='/starships/:id'
                                       render={({match}) => {
                                           //react передает в роут объект с параматрами match, local, history
                                           // в  match есть поле id (как и url)
                                           const {id} = match.params;
                                           return <StarshipDetails itemId={id}/>
                                       }}/>

                                <Route path='/login' render={() => {
                                    return <LoginPage isLoggedIn={isLoggedIn} onLogin={this.onLogin}/>
                                }}/>
                                <Route path='/secret' render={() => {
                                    return <SecretPage isLoggedIn={isLoggedIn}/>
                                }}/>

                                {/*Если ни один из раутов не сработает, то сработает последний без поля path,
                                из-за switch*/}
                                <Route render={() => <h3>Page not found</h3>} />
                            </Switch>

                        </div>
                    </Router>
                </SwapiServiceProvider>
            </ErrorBoudry>
        );
    }
};