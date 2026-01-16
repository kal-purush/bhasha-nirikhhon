import React from 'react';

export default class UserCard extends React.Component{
    constructor(){
        super();
        this.state = {
            user: {}
        }
        this.componentDidMount = this.componentDidMount.bind(this);
    }

    componentDidMount(){
        this.state.user = this.props.userInfo;
    }

    render(){

        return(
            <>
            <h1>{this.state.user.login}</h1>
            </>
        )
    }
}