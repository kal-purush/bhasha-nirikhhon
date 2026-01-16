import React from 'react';
import ReactDOM from 'react-dom';
// import {Link} from 'react-router-dom';
import EventListEntry from './eventListEntry'

class EventList extends React.Component{
    constructor(props) {
        super(props);
        this.state ={ 

        }
        console.log(this.props.events)
        }

render() {
return ( 
    // !this.props.event? <div>Loading</div> :
    <div>
   
    <div className ="video-list">
    {this.props.events.map((event,i) => (
        <EventListEntry event={event} key={i}/>
    ))}  
</div>
</div>
)
 }
}

export default EventList; 