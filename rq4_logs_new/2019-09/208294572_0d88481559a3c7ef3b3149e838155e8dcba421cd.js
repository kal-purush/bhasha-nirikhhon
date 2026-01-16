import React from "react";
import { Switch, Route } from "react-router-dom";
import Form from "./components/Form/Form";
import Dashboard from "./components/Dashboard/Dashboard";
import Product from './components/Product/Product'

export default (
  <Switch>
    <Route exact path="/" component={Dashboard} />
    <Route path="/Inventory" component={Form} />
    <Route path='/Product' component={Product}/>
  </Switch>
);