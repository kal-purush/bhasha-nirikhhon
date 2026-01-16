import React from "react";

import { BrowserRouter as Router, Route, Switch } from "react-router-dom";

import AddVehicle from "./pages/AddVehicle";
import CheckInToll from "./pages/CheckInToll";
import CheckOutToll from "./pages/CheckOutToll";
import Login from "./pages/Login";
import Register from "./pages/Register";
import Index from "./pages/Index";
import UserDashboard from "./pages/UserDashboard";
import TollDashboard from "./pages/TollDashboard";
import PrivateRoute from "./Component/PrivateRoute";
import Receipt from "./pages/Receipt";
import AddToll from "./pages/AddToll";

function App() {
  return (
    <React.Fragment>
      <Router>
        <Switch>
          <Route path="/" component={Index} exact />
          <Route path="/login" component={Login} />
          <Route path="/register" component={Register} />
          <Route path="/toll-dashboard" component={TollDashboard} />
          <Route path="/toll-add" component={AddToll} />
          <PrivateRoute path="/user-dashboard" component={UserDashboard} />
          <PrivateRoute path="/check-into-toll" component={CheckInToll} />
          <PrivateRoute path="/check-out-toll" component={CheckOutToll} />
          <PrivateRoute path="/add-vehicle" component={AddVehicle} />
          <PrivateRoute path="/add-vechicle" component={AddVehicle} />
          <PrivateRoute path="/receipt" component={Receipt} />
        </Switch>
      </Router>
    </React.Fragment>
  );
}

export default App;