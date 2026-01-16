import React from 'react';
import './Login.css';
import { Switch, Route } from 'react-router-dom'
import { Input, Form, FormGroup, Label, Button,CardDeck } from 'reactstrap';
import GraphQLRequest from "../graphQLUtils";

export default class Login extends React.Component {
    constructor(props){
        super(props)
        this.state = {
            isLoading: 1,
            email:'',
            password:''
        }
        this.handleChange = this.handleChange.bind(this);
        this.handleSubmit = this.handleSubmit.bind(this);
    }

    handleChange = (event)=> {
        this.setState({
            [event.target.name]:event.target.value
        });
    }

    handleSubmit(event) {

        // console.log(this.state.dataSource.filter((e) => e.leccion === parseInt(this.props.match.params.id, 10)).map(p => (p.id)))
        console.log("   dfsdf fd")
        alert('email ' + this.state.email);
        alert('email ' + this.state.password);
    }

    render(){
        return(

            <Form className={"form-sigin"} onSubmit={this.handleSubmit}>
                <h1 className="h3 mb-3 font-weight-normal">Please sign in</h1>
                <Label htmlFor="inputEmail" className={"sr-only-4"}>
                    Email address
                </Label>
                <Input value={this.state.value} onChange={this.handleChange} type="email" placeholder="Email address" name="email" className="form-control" id="inputEmail" required autoFocus />
                <Label htmlFor="inputPassword" className="sr-only">
                    Password
                </Label>
                <Input value={this.state.value} onChange={this.handleChange} type="password" placeholder="Password" name="password" className="form-control" id="inputPassword" required />
                <Button className="btn-secondary " type="submit" value="Submit">Sign in</Button>
            </Form>

        );
    }
}