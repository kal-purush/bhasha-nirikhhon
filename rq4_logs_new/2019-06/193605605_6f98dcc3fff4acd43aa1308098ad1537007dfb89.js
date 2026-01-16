import React from "react";
import axios from "axios";

class EditFriend extends React.Component {
  constructor() {
    super();
    this.state = {
      name: "",
      email: "",
      age: "",
      errorMessage: null
    };
  }

  // need to populate the data as soon as the page loads
  componentDidMount() {
    const id = this.props.match.params.id;
    axios
      .get(`http://localhost:5000/friends/${id}`)
      .then(response => {
        this.setState({
          name: response.data.name,
          email: response.data.name,
          age: response.data.name
        });
      })
      .catch(error => {
        this.setState({
          errorMessage: error.response.data.error
        });
      });
  }

  handleChange = event => {
    this.setState({
      [event.target.name]: event.target.value
    });
  };

  deleteFriend = event => {};

  // following along with PUT Requests with Axios video
  changeFriend = event => {
    event.preventDefault();

    const { name, age, email } = this.state;
    const updatedFriend = { name, age, email };
    const id = this.props.match.params.id;

    axios
      .put(`http://localhost:5000/friends/${id}`, updatedFriend)
      .then(response => {
        this.setState({ errorMessage: null });
        this.props.updateFriends(response.data);
      })
      .catch(error => {
        this.setState({ errorMessage: error.response.data.error });
      });
  };

  render() {
    const { name, age, email } = this.state;

    return (
      <form onSubmit={this.changeFriend}>
        <h1>Edit Friend</h1>
        <input
          type='text'
          name='name'
          placeholder='Name'
          value={name}
          onChange={this.handleChange}
        />
        <input
          type='text'
          name='email'
          placeholder='Email'
          value={email}
          onChange={this.handleChange}
        />
        <input
          type='text'
          name='age'
          placeholder='Age'
          value={age}
          onChange={this.handleChange}
        />

        <button type='submit'>Save Your Edit</button>
        <button onClick={this.deleteFriend}>Delete</button>
      </form>
    );
  }
}

export default EditFriend;