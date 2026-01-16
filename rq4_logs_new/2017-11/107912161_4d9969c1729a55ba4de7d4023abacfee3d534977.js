import React, { PureComponent } from 'react';
import moment from 'moment';

class Slider extends PureComponent {

    constructor(props) {
        super(props);

        this.state = {
            hour: this.props.hour ? this.props.hour : moment().format('HH'),
            min: this.props.min ? this.props.min : moment().format('mm'),
            _moment: moment(),
            data: [],
            up: 0
        }

        this.updateHour = this.updateHour.bind(this);
        this.updateMin = this.updateMin.bind(this);
        this.newAlarm  = this.newAlarm.bind(this);
        this.deleteEvent = this.deleteEvent.bind(this)
    }



    updateHour(e) {    
        let {_moment} = this.state;
        return this.setState({
            hour: e.target.value,
            _moment: _moment.hour(e.target.value)
        })
    }

    updateMin(e) {
        let {_moment} = this.state;
        return this.setState({
            min: e.target.value,
            _moment: _moment.minute(e.target.value)
        })
    }

    newAlarm(e) {
        this.state.data.push(this.state._moment);

        return this.setState({
            hour: this.props.hour ? this.props.hour : moment().format('HH'),
            min: this.props.min ? this.props.min : moment().format('mm'),
            _moment: moment(),
        })
    } 

    deleteEvent(item) {
        const newState = this.state.data;

        let _m = moment();

        if( newState.indexOf(item) > -1) {
            newState.splice(newState.indexOf(item), 1);
        }

        return this.setState({
            data: newState,
            up:  _m.format('mm')+_m.format('ss')+_m.format('mss'),
        })
    }
    


    render() {

        const {hour,min,_moment,data} = this.state;


        const items = data.map((item,i) => {
            return <li key={i}>
                      {item.format('HH')}:{item.format('mm')}
                      <button type="button" onClick={this.deleteEvent.bind(this, item)}>Delete</button>
                  </li>
        })

        return (
            <div>
                <p>{_moment.format('HH')}:{_moment.format('mm')}</p>
                <input type="range" step="1" min="0" max="23" value={hour} name="hour" onChange={this.updateHour} /> <br />
                <input type="range"  min="0" max="59" value={min} name="min" onChange={this.updateMin} />  <br />
                <button type="button" onClick={this.newAlarm} >Set Alarm</button>

                <br />
                <h3>Alarms</h3>
                <ul>
                    {items}
                </ul>
            </div>
        );
    }
}

export default Slider;