import React from 'react'
import io from 'socket.io-client'
let socket = io('http://localhost:3000')

import App from 'grommet/components/App';
import Header from 'grommet/components/Header';
import Title from 'grommet/components/Title';
import Menu from 'grommet/components/Menu';
import Anchor from 'grommet/components/Anchor';
import Box from 'grommet/components/Box';

import NavLink from './Navlink.jsx'

import 'grommet/grommet-hpinc.min.css'
import './style.css'






export default class Main extends React.Component {
	
	render(){
		console.log(this.state)
		return(
			<div>
				<Header size="small" justify="between" colorIndex="neutral-1" pad={{"horizontal": "medium"}}>
				  <Title>
				    SleepC
				  </Title>

				  <Menu inline={true} direction="row">
				     <NavLink style={styles.navLink} to="/" onlyActiveOnIndex={true}> Dashboard </NavLink>
				  	 <NavLink style={styles.navLink} to="/reports"> Reports </NavLink>		    
				  </Menu>		 
				</Header>

				<App>
					{this.props.children}
				</App>
			</div>
			
		)
	}
}

const styles = {
	navLink: {
		color: '#ddd',
		
	}
}