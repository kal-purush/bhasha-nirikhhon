import React, {Component} from 'react'
import {Link} from 'react-router-dom';

//stylesheets
import './Wizard.css'

//redux
import store {} from 

export default class Step1 extends Component {
    constructor(){
        super();
        this.state = {
            name: '',
            address: '',
            city: '',
            state: '',
            zip: 0
        }
    }

    handleOnChange = (event) => {
        this.setState({
            [event.target.name]: event.target.value
        })
    }
    render() {
        return (
                <div className='wizard-conatiner'>
                    <div className='wizard-sub-header'>
                    <h1>Add New Listing</h1>
                    </div>
                    <label>Property Name:</label>
                    <input type='text' name='name' onChange={this.handleOnChange} value={this.state.name}/>
                    <label>Address:</label>
                    <input type='text' name='address' onChange={this.handleOnChange} value={this.state.address}/>
                    <label>City:</label>
                    <input type='text' name='city' onChange={this.handleOnChange} value={this.state.city}/>
                    <label>State:</label>
                    <input type='text' name='state' onChange={this.handleOnChange} value={this.state.state}/>
                    <label>Zipcode:</label>
                    <input type='text' name='zip' onChange={this.handleOnChange} value={this.state.zip}/>
                    <Link to='/wizard/step2'><button>Next Step</button></Link>
                </div>
        )
    }
}