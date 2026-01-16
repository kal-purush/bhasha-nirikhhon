// Core
import React, { Component } from 'react';
import { func } from 'prop-types';

//Instruments
import Styles from './styles';


export default class Header extends Component {

    static propTypes = {
        search:     func.isRequired
    };

    constructor () {
        super();
    }

    state = {
    };

    _handleSearch = ({ target }) => {
        const { value: term } = target;

        this.props.search(term);
    };


    render () {
        const { term, search } = this.state;

        return (
            <section className = { Styles.header } >
                <h1>To Do List</h1>
                <form>
                    <input
                        placeholder = { 'Search' }
                        type = 'text'
                        onChange = { this._handleSearch }
                    />
                </form>
            </section>
        );
    }
}