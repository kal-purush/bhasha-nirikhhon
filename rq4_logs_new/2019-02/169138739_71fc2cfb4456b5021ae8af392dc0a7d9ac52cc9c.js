import React, {Component, Fragment} from 'react';
import ReactDOM from 'react-dom';
import Flatpickr from 'react-flatpickr'

class App extends Component{

    constructor() {
        super();
        
        this.state = {
            date: new Date()
        };
    }

    render()
    {
        const {date} = this.state;
        return (
            <Fragment>
                <Flatpickr 
                    data-inline
                    options={{enable:['2019-02-22']}}
                    value={date}
                    onChange={date => { this.setState({date}) }} />
            </Fragment>
        )
    }
}


ReactDOM.render(
    <App />,
    document.getElementById('app')
  );