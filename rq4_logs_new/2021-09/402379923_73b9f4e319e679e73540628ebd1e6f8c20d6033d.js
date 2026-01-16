import { Component } from "react";
import { v4 as uuidv4 } from 'uuid';
import ContactList from "./components/ContactList/ContactList";
import Filter from "./components/Filter/Filter";
import Form from "./components/Form/Form";
import './App.css'


class App extends Component {
  state = {
    contacts: [
      { id: 'id-1', name: 'Rosie Simpson', number: '459-12-56' },
      { id: 'id-2', name: 'Hermione Kline', number: '443-89-12' },
      { id: 'id-3', name: 'Eden Clements', number: '645-17-79' },
      { id: 'id-4', name: 'Annie Copeland', number: '227-91-26' },
    ],
    filter: ''
  };

  addContact = ({ name, number}) => {
    const newContact = {
      id: uuidv4(),
      name: name,
      number: number
    }
    const searchName = this.state.contacts.map(contact => contact.name).includes(name);
    
    if (searchName) {
      alert(`${name} is already in contacts`)
      return
    }

    this.setState(prevState => (
      {contacts: [newContact, ...prevState.contacts]}))
  };

  deleteContact = id => {
    this.setState(({ contacts }) => ({
      contacts: contacts.filter(contact => contact.id !== id)
    }))
  };

  filterContacts = (e) => {
    this.setState({
      filter: e.target.value
    })
  };

  getVisibleContacts = () => {
    const normalizedFilter = this.state.filter.toLowerCase();
    return this.state.contacts.filter(contact => contact.name.toLowerCase().includes(normalizedFilter))
  };

  render() {
    return (
      <div className="App">
        <h1>Phonebook</h1>
        <Form onSubmit={this.addContact}/>
        <h2>Contacts</h2>
        <Filter value={this.state.filter} onChange={this.filterContacts}/>
        <ContactList contacts={this.getVisibleContacts()} onDeleteContacts={this.deleteContact} />
      </div>
    );
  }
 
}

export default App;