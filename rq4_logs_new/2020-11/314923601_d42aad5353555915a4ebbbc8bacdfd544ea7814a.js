import React from "react";
import "./App.css";
import { BrowserRouter as Router, Route, Switch } from "react-router-dom";
import Home from "./components/Home";
import Login from "./components/Login";
import Register from "./components/Register";
import ProfilePage from "./components/ProfilePage";
import NoMatch from "./components/NoMatch";
import UpdateProfile from "./components/UpdateProfile"

function App() {
	return (
		<Router>
			<Switch>
				<Route exact path="/" component={Home} />
				<Route path="/login" component={Login} />
				<Route path="/register" component={Register} />
				<Route path="/update" component={UpdateProfile} />
				<Route path="/profile/:username" component={ProfilePage} />
				<Route component={NoMatch} />
			</Switch>
		</Router>
	);
}

export default App;