import React, { Component } from 'react';
import { auth } from '../firebase';
import * as helper from '../constants/helper';
import {
    Card,
    Form,
    Button
  } from 'semantic-ui-react';

const byPropKey = (propertyName, value) => () => ({
    [propertyName]: value,
});


class ProfileForm extends Component {
    constructor(props){
        super(props);
        const player = helper.WhoAmI(this.props.season, this.props.user);
        this.state = { 
            displayName: this.props.user.displayName,
            player: player,
            error: null,
        };
    }

    onSubmit = (event) => {
        const profile = {displayName: this.state.displayName}
        auth.doUpdateProfile(profile)
            .then(()=> {
                console.log('profile updated')
            })
            .catch(error => {
                this.setState(byPropKey('error',error));
            });
        event.preventDefault();
    }
    handleChange(e){
        let change = {}
        change[e.target.name] = e.target.value
        this.setState(change)
    }
    render(){
        const {
            user
        } = this.props;
        const {
            displayName,
            player,
            error,
        } = this.state;
        const isInvalid = displayName === '';

        return(
            <Card fluid>
                <Card.Content>
                    <Card.Header>Update my profile</Card.Header>
                    <Card.Description>
                        <Form size='large' onSubmit={this.onSubmit}>
                            <Form.Field>
                                <label>Display Name</label>
                                <input placeholder='Your Name' name='displayName' value={displayName} onChange={this.handleChange.bind(this)} />
                            </Form.Field>
                            <Form.Field>
                                <label>Driver 1</label>
                                <input placeholder='Your Name' name='driver1' value={`${player.Driver1.givenName} ${player.Driver1.familyName}`} disabled />
                            </Form.Field>
                            <Form.Field>
                                <label>Driver 2</label>
                                <input placeholder='Your Name' name='driver2' value={`${player.Driver2.givenName} ${player.Driver2.familyName}`} disabled />
                            </Form.Field>
                            <Button color='green' fluid size='large' type="submit" disabled={isInvalid}>
                            Update my profile
                            </Button>
                            { error && error.message }
                        </Form>
                    </Card.Description>
                </Card.Content>
            </Card>
        );
    }
}

export default ProfileForm;