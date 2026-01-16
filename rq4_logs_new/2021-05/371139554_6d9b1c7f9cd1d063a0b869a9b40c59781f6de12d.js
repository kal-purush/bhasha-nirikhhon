import "./App.css";
import axios from "axios";
import Button from "react-bootstrap/Button";

import React, { Component } from "react";

export default class App extends Component {
  constructor(props) {
    super(props);
    this.state = {
      userData: [],
      loading: false,
    };
  }

  fetchUserData = async () => {
    this.setState({
      loading: true,
    });
    var data = await axios.get("https://reqres.in/api/users?page=1");
    this.setState({
      userData: data.data.data,
      loading: false,
    });
  };

  render() {
    return (
      <div>
        <nav className="navbar">
          <h2 className="navbar__brand">Advertyzement</h2>
          <Button
            classname="navbar__button"
            variant="outline-warning"
            onClick={this.fetchUserData}
          >
            Get Users
          </Button>
        </nav>
        <div className="cards">
          {this.state.userData.map((data, key) => (
            <div className="card" key={data.id}>
              <div className="cardImg">
                <img src={data.avatar} alt={data.first_name} />
              </div>
              <div className="cardData">
                <p>
                  First Name: <span>{data.first_name}</span>
                </p>
                <p>
                  Last Name: <span>{data.last_name}</span>
                </p>
                <p>
                  Email: <span>{data.email}</span>
                </p>
              </div>
            </div>
          ))}
        </div>
      </div>
    );
  }
}