import React from 'react';
import { BrowserRouter, Route, Switch } from 'react-router-dom';

import Login from './login';
// import Register from './register';
import Home from './home';

class Router  extends React.Component {
    render() {
        return(
            <BrowserRouter>
                <Switch>
                    <Route exact path="/" component={Login} />
                    <Route path="/login" component={Login} />
                    {/* <Route path="/register" component={Register} /> */}
                    <Route path="/home" component={Home} />
                </Switch>
            </BrowserRouter>
        )
    }
};

export default Router;