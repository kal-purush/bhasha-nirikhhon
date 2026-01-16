import React, { Component } from 'react'
import '../css/CakeSize.css'

class CakeSize extends Component {
    render() {
        return (
            <div id="sizebtns">
                <input type="button" value="6" onClick={this.props.sizeHandler} />
                <input type="button" value="8" onClick={this.props.sizeHandler} />
                <input type="button" value="9" onClick={this.props.sizeHandler} />
                <input type="button" value="18*32" onClick={this.props.sizeHandler} />
            </div>
        )
    }
}

export default CakeSize