//***************************************************
//    career-app.js    Author: Austin George
//    Holds everything inside
//***************************************************

/*Tasks:
	- Show/Hide Respective Targets
*/

import React from 'react';
import { compose } from 'redux'
import { connect } from 'react-redux'
import {firebaseConnect, isLoaded, isEmpty} from "react-redux-firebase";
import { DragDropContext } from 'react-dnd';
import HTML5Backend from 'react-dnd-html5-backend';
import FieldPanel from './field-panel.js';
import CareerPanel from './career-panel.js';
import DraggableTarget from './draggable-target.js';

class CareerApp extends React.Component {
	state = {
		targets: [
			{ prompt: 'You Are Here', career: '', field: ''},
			{ prompt: "In the future, I'd like too ...", career: '', field: ''},
		],
	}

	constructor(props){
		super(props);

	};

	//Executed whenever a field/career is selected
	updateTarget = (target, type, name) => {
		console.log('target' + target);
		console.log('type: ' + type);
		console.log('name: ' + name);

		this.setState(prevState => {
			let targets = prevState.targets;
			if(type == 'career')
			{
				targets[target].career = name;
			}
			else if (type == 'field')
			{
				console.log('CareerApp: Got inside field');
				targets[target].field = name;
				console.log('New Target Field' + targets[target].field);
			}

			//Upon receiving this info, additional timeline items can be inserted HERE
			return targets;
		});

	}
	render(){

		

		return(
				<div className="careerApp">
					<FieldPanel handleDrop={(target, type, name) => this.updateTarget(target, type, name)}/>
					<CareerPanel handleDrop={(target, type, name) => this.updateTarget(target, type, name)}/>
					<div id="inline">
					{this.state.targets.map((item, index) => (
						<DraggableTarget prompt={item.prompt} index={index} career={item.career} field={item.field}/>
						))}
					</div>
				</div>
			)

	}
}

export default DragDropContext(HTML5Backend)(CareerApp);