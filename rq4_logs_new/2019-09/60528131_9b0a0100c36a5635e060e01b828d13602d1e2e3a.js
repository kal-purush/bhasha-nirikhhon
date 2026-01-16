import React, { Component } from 'react';

export class Login extends Component {

    constructor(props) {
        super(props);
        this.state = {
            wrongLogin: false,
        }
    }
    
    login = (event) => {
        event.preventDefault();
        const username = this.inputUsername.value;
        const password = this.inputPassword.value;
        Meteor.loginWithPassword(username, password, (error) => {
            if (error) {
                this.setState({
                    wrongLogin: true,
                });
            } else {
                this.setState({
                    wrongLogin: false,
                });
            }
        });
    }

    logout = (event) => {
        event.preventDefault();
        Meteor.logout(err => {
            if (err) {
                console.log(err.reason);
            }
        });
    }

    render() {
        const formState = ''
        const wrongLogin = this.state.wrongLogin ? <div id="wrong-login">Gebruikersnaam of wachtwoord onjuist!</div>: null
        const gebruiker = Meteor.user()
        
        return (
            <div>
                {
                    gebruiker ? 
                    <div>
                        <div>{gebruiker.username}</div>
                        <button id="logout-button" className="btn btn-primary" onClick={this.logout} >Uitloggen</button>
                    </div> :
                    <form id="login-form" onSubmit={this.login}>
                        <div className="form-group">
                            <label htmlFor="inputUsername">Username</label>
                            <input type="text" className="form-control" ref={(input) => this.inputUsername = input} id="inputUsername" placeholder="Username" required disabled={formState}/>
                        </div>
                        <div className="form-group">
                            <label htmlFor="inputPassword">Password</label>
                            <input type="password" className="form-control" ref={(input) => this.inputPassword = input} id="inputPassword" placeholder="Password" required disabled={formState}/>
                        </div>
                        {wrongLogin}
                        <button type="submit" className="btn btn-primary" style={{display: "block"}} >LOGIN</button>
                    </form>
                }
            </div>
        );
    }
}