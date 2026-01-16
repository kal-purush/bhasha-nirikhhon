import React, { Component } from 'react'
import CardList from './CardList';
import SearchBox from './SearchBox'
import Scroll from './Scorll'
import './App.css'


class App extends Component {
    constructor() {
        super();
        this.state = {
            robots: [],
            searchField: ''
        }
    }

    componentDidMount = () => {
        fetch(`${process.env.PUBLIC_URL}/data/robots.json`)
            .then(response => response.json())
            .then(users => {
                console.log('users', users);
                this.setState({ robots: users });
            })
    }

    onSearchChange = event => {
        this.setState({ searchField: event.target.value });
    }

    render() {
        const filteredRobots = this.state.robots.filter(robot => {
            return robot.name.toLowerCase().includes(this.state.searchField.toLowerCase());
        });
        if (this.state.robots.length === 0) {
            return <h1>Loading...</h1>
        } else {
            return (
                <div className="tc">
                   
                    <SearchBox searchChange={this.onSearchChange} />
                    <Scroll>
                        <CardList robots={filteredRobots} />
                    </Scroll>
                </div>
            )
        }

    }

}

export default App