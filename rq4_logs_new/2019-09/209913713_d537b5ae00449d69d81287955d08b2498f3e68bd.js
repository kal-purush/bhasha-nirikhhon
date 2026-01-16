import React, { Component } from 'react';
import { Link } from 'react-router-dom';
import axios from 'axios';
const Contacts = props => (
    <tr>
    <td>{props.contacts.first_name}</td>
    <td>{props.contacts.last_name}</td>
    <td>{props.contacts.phone}</td>
    <td>{props.contacts.email}</td>
    <td>{props.contacts.nickname}</td>
    <td>{props.contacts.birthday}</td>
    {/* <td>{props.contacts.date.substring(0,10)}</td> */}
    <td>
            {/* <Link to={"/edit/"+props.contact.owner}>Edit</Link> */}
        </td>
    </tr>
)
export default class ContactList extends Component {
    constructor(props) {
        super(props);
        this.state = {contacts: []};
    }
    componentDidMount() {
		const owner = { owner :'antoinedga' }

        axios.post('/api/contacts/dashboard', owner)
            .then(response => {
					console.log(response)
                this.setState({ contacts: response.data });
            })
            .catch(function (error){
                console.log(error);
            })
    }
    ContactList() {
        return this.state.contacts.map(function(currentContact, i){
            return <Contacts contacts={currentContact} key={i} />;
        })
    }
    render() {
        return (
            <div>
                <h3>Contact</h3>
                <table className="table table-striped" style={{ marginTop: 20 }} >
                    <thead>
                        <tr>
								<th>First Name</th>
								<th>Last Name</th>
								<th>Phone Number</th>
								<th>Email</th>
								<th>Nickname</th>
								<th>Birthday</th>
								<th>Date Created</th>
                        </tr>
                    </thead>
                    <tbody>
                        { this.ContactList() }
                    </tbody>
                </table>
            </div>
        )
    }
}