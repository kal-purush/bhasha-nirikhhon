import React, { Component } from 'react';

class WorkSample extends Component {
	render() {
		return(
			<div className="work-sample mb3">
				<div className="flex items-center p2">
				  <h4 className="h3 medium my0 flex-auto">{this.props.category}</h4>
				  <a className="h5 text-decoration-none" target="_blank" href={this.props.src}>Download</a>
				</div>
				<audio controls>
				  <source src={this.props.src} type="audio/mpeg"/>
				  Your browser does not support the audio element.
				</audio>
			</div>
		);
	}
}

export default WorkSample;