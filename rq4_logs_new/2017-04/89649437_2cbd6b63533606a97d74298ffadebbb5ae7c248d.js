import React, { Component } from 'react';
import restaurantData from './data';


class MenuSelect extends Component {
  state = {
    spiceThresh: 10,
  }
	textChange(event){
		this.setState({spiceThresh: Number(event.target.value)});
	}
	render(){
    console.log(restaurantData);
		return(
			<div className="container">
				<h1>Your goal here is to:</h1>
				<ul>
				<li>1. Develop a restaurant menu interface</li>
				<li>2. Allow users to enter in a spice level
					 to a text field</li>
				<li>3. Use input data to filter the given menu
					 items</li>
					<li>4. You must place your input interface within a separate
					 "dumb" react component and call it here</li>
					<li>NOTE: If no input is entered, all menu items should display</li>
				</ul>
				<img src="http://vignette2.wikia.nocookie.net/bookoflife/images/7/73/All_you_can_eat_churros.PNG/revision/latest?cb=20140910221451"/>
			</div>
		)
	}
}


export default MenuSelect;