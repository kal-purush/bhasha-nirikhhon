import React, { Component } from 'react';
import TodoContact from './components/TodoContact';
import ContactsList from './components/ContactsList';
import Filter from './components/Filter';

import shortid from 'shortid';

class App extends Component {
  state = {
    contacts: [],
    filter: '',
  };

  componentDidMount() {
    const localContacts = localStorage.getItem('contacts')
    const parsedContacts = JSON.parse(localContacts)

    if (parsedContacts) {
      this.setState({ contacts: parsedContacts })
    }
  }

  componentDidUpdate(prevProps, prevState) {
    if (this.state.contacts !== prevState.contacts) {
      localStorage.setItem('contacts', JSON.stringify(this.state.contacts))
    }
  }

  addContact = (name, number) => {
    const todo = {
      id: shortid.generate(),
      name,
      number,
    };
    const { contacts } = this.state;
    const alreadyInContacts = contacts.find(
      contact => contact.name === todo.name && contact.number === todo.number,
    );
    if (alreadyInContacts) {
      alert(`${alreadyInContacts.name} is already in contacts.`);
      return;
    }
    if (todo.name === '') {
      alert('Please enter data');
      return
    }
    if (todo.number === '') {
      alert('Please enter the number')
      return
    }
    this.setState(prevState => ({
      contacts: [todo, ...prevState.contacts],
    }));
  };

  ChangeFilter = e => {
    const { value } = e.currentTarget;

    this.setState({ filter: value });
  };

  deleteContact = todoId => {
    this.setState(prevState => ({
      contacts: prevState.contacts.filter(todo => todo.id !== todoId),
    }));
  };

  render() {
    const normalizedFilter = this.state.filter.toLowerCase();
    const visibleTodos = this.state.contacts.filter(todo =>
      todo.name.toLowerCase().includes(normalizedFilter),
    );

    return (
      <>
        <TodoContact onSubmit={this.addContact} />
        <Filter value={this.state.filter} onChange={this.ChangeFilter} />
        <ContactsList
          contacts={visibleTodos}
          onDeleteContact={this.deleteContact}
        />
      </>
    );
  }
}

export default App;