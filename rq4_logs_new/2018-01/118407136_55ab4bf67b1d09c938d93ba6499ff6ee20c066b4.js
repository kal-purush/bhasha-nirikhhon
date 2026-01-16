import React, { Component } from 'react';
import XSView from './XS/XSView';
import NormalView from './Normal/NormalView'

export default class MainScreen extends Component {
    constructor(props) {
        super(props);
        
        this.state = {
            isMobile: false
        };
        
        this._setIsMobile = this._setIsMobile.bind(this)
    }

    componentDidMount() {
        window.addEventListener("resize", this._setIsMobile)
    }

    componentWillUnmount() {
        window.removeEventListener("resize", this._setIsMobile)
    }

    render() {
        let view = this.state.isMobile ? <XSView/> : <NormalView/>;
        return (
            <div>
                {view}
            </div>
        )
    }

    _setIsMobile() {
        let width = window.innerWidth;
        if (width < 700 && !this.state.isMobile) {
            this.setState({
                isMobile: true
            })
        } else if (width > 700 && this.state.isMobile) {
            this.setState({
                isMobile: false
            })
        }
    }
}