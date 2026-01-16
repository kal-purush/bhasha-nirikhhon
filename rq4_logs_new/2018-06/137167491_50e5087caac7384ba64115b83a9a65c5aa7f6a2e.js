import React from "react";
import { Router, Route, browserHistory} from "react-router";
import { createApp } from "@phenomic/preset-react-app/lib/client";

const Home = () => (
	<div>
		<p>Hello hungry people!</p>
	</div>
);

export default createApp(() => (
	<Router history={browserHistory}>
		<Route path="/" component={Home} />
	</Router>
));