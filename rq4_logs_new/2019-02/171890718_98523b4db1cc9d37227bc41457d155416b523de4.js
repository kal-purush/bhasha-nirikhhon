import React, { Component } from 'react';
import { Mutation } from 'react-apollo';
import gql from 'graphql-tag';
import Form from './styles/Form';
import Error from './ErrorMessage';

export default class Signup extends Component {

    state = {
        name: '',
        password: '',
        email: ''
    };

    saveToState = (e) => {
        this.setState({[e.target.name]: e.target.value})
    }
    render() {
        return (
            <Form>
                <fieldset>
                    <h2>Sign Up for an Account</h2>
                    <label htmlFor="email">
                        Email
                        <input type="email" name="email" placeholder="Email" value={this.state.email} onChange={this.saveToState}/>
                    </label>
                    <label htmlFor="name">
                        Name
                        <input type="text" name="name" placeholder="Name" value={this.state.name} onChange={this.saveToState} />
                    </label>
                    <label htmlFor="password">
                        Password
                        <input type="password" name="password" placeholder="Password" value={this.state.password} onChange={this.saveToState} />
                    </label>
                    <button type="submit">Sign Up!</button>
                </fieldset>
            </Form>
        )
    }
}