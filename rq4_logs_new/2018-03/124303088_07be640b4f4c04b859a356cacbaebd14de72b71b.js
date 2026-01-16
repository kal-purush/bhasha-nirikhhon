import React, { Component } from 'react'
import PropTypes from 'prop-types'
import {connect} from 'react-redux'
import {Link} from 'react-router-dom'
import {logout, getCar} from '../store'



export class SingleCarView extends Component {
    constructor(props) {
        super(props);
    }
    
    componentDidMount() {
        const carId = this.props.match.params.carId;
        this.props.getCar(carId);
    }
    render () { 
        return (
            <div>
                {this.props.car && 
                this.props.car.id && 
                (<div key={this.props.car.id} >
                        <h2> Car Model: {this.props.car.name} </h2>
                        <p> Description: {this.props.car.description} </p>
                        <h5> Views: {this.props.car.views} </h5>
                </div>)
                
                }
            </div>
        )}
}


const mapState = state => {
    return {
      isLoggedIn: !!state.user.id,
      car: state.base.car
    }
  }
  
  const mapDispatch = dispatch => {
    return {
      handleClick() {
        dispatch(logout())
      },
      getCar(id){
        dispatch(getCar(id))
      }
    }
  }


export default connect(mapState, mapDispatch)(SingleCarView)
