import React, {Component} from 'react';

import {BrowserRouter as Router,Switch,Route} from 'react-router-dom'
import LayoutModel from './components/layout/index'
import Home from './pages/home/index'
import customerList from './pages/coustomer/list'
import './App.css';
import "antd/dist/antd.css";

class App extends Component {
    render() {
        return (
            <Router>
                <LayoutModel>

                    <Switch>
                        <Route path="/" exact component={Home}></Route>
                        <Route path="/customer" component={customerList}></Route>
                    </Switch>
                </LayoutModel>
            </Router>
        )

    }
}

export default App;