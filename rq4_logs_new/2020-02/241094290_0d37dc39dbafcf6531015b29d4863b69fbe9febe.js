import React, { Component } from 'react'
import './css/app.scss'

export default class App extends Component
{
    constructor(props)
    {
        super(props);
        
        this.state = 
        {
            loading: true
        }
    }
    
    render()
    {
        return (
            <div className="App">
                
            </div>
        );
    }
}