import React,{Component} from 'react';
import ServiceItem from './ServiceItem.js';
import {
	Group,
	List
} from 'amazeui-touch';

class ServiceGroup extends Component{
	constructor(props){
		super(props);
		console.log(props);
		this.state = props;
	}
	render(){
		return (<div>
			<Group
          		header={this.state.group.name}
          		noPadded
        	>
        	<List>
        	{
        		this.state.group.services.map((srv,idx)=>{
        			return <ServiceItem item={srv} key={idx} />
        		})
        	}
        	</List>
        	</Group>
		</div>);
	}
}

export default ServiceGroup;