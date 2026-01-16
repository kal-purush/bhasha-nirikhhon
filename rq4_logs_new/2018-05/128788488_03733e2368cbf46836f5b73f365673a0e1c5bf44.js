import React, { Component } from "react";

// import { animationData } from "./puppy_run.js";
import animationData from "./stella_dance.json";
import ReactBodymovin from "react-bodymovin";

export default class BodymovinAnimation extends Component {

	constructor(props) {
		super(props);
	}

	/* Triggered before component loads */
	componentWillMount() {}

	/* Triggered when component loads */
	componentDidMount() {
	}

	componentWillUnmount() {
	}


	render() {
		const bodymovinOptions = {
			loop: true,
			autoplay: true,
			prerender: false,
			animationData: animationData
		};

		return (
			<div id={"bodymovin-container"}>
				<ReactBodymovin options={bodymovinOptions} />
			</div>
		);
	}
}