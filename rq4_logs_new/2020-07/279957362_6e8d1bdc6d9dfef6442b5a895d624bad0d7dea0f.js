// Question: 5. What is Robert Baratheon's second alias?

import React, { Component } from 'react';
import axios from 'axios';

export default class FifthQuestion extends Component {
    constructor (props) {
        super (props)

        this.state = ({
            info: []
        });
    }

    componentDidMount () {
        // retrieving data (in this case a single object) from the API link
        axios.get("http://www.anapioficeandfire.com/api/characters/901")
        .then(res => {
            // setState so "info" carries the correct answer to the question
            this.setState({ info: res.data.aliases[1] })
        })
        .catch(e => console.error(e.message))

    }
    render() {
        return (
            <div>
                <h3>Question 5:</h3>
                <h4 className='question'>What is Robert Baratheon's second alias?</h4>
                {/* {console.log(this.state.info)} */}
                <p className='answer'>{ this.state.info }</p>
            </div>
        )
    }
}