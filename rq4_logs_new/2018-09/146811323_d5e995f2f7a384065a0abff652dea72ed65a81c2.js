import React, { Component } from 'react';
import axios from 'axios';

import { Link } from 'react-router-dom';

let theEmailId = window.location.href.split('').reverse().join('');
const emailI = theEmailId.indexOf('/');

theEmailId = theEmailId.substring(0, emailI);
theEmailId = theEmailId.split('').reverse().join('');

const axiosUrl = '/api/emails/' + theEmailId;


class EmailEdit extends Component {


  state = {
    email: []
  }

  componentDidMount() {
    axios.get(axiosUrl).then((res) => {
      this.setState({ email : res.data })
      console.log(res.data);
    })
  }

  handleChange(event) {
    this.setState({value: event.target.value});
  }


  render() {
    return (
      <div className="row-this higher">

        <div className="container formFlex">

          <div className="inside">

            <form method="POST" action={'/api/email/edit/' + theEmailId + '/?_method=PUT'}>
              <input type="text" placeholder="Campaign Title" name="title" onChange={(event) => {this.setState({email: {title: event.target.value}})}} value={this.state.email.title}></input>
              <input type="text" placeholder="Subject" name="subject" onChange={(event) => {this.setState({email: {subject: event.target.value}})}} value={this.state.email.subject}></input>
              <input type="text" placeholder="Body" name="body" onChange={(event) => {this.setState({email: {text: event.target.value}})}} value={this.state.email.text}></input>
              <input type="text" placeholder="Recipients" name="recipients" onChange={(event) => {this.setState({email: {to: event.target.value}})}} value={this.state.email.to}></input>




              <Link to="/dashboard" className="red btn-flat white-text hoverable waves-effect waves-light">
                Cancel
              </Link>
              <button type="submit" className="green white-text btn-flat right waves-effect waves-light">
                Save Email
                <i className="material-icons right">event_available</i>
              </button>

            </form>

          </div>

        </div>
      </div>
    );
  }
}

export default EmailEdit