import React, { Component } from 'react';

class Profile extends Component {
  constructor(props) {
    super(props);

    this.state = {
      notes: [],
      bio: {},
      repos: []
    }
  }

  render() {
    return (
      <div className='row'>
        <div className='col-md-4'>
          User Profile Component
        </div>
        <div className='col-md-4'>
          Repos Component
        </div>
        <div className='col-md-4'>
          Notes Component
        </div>
      </div>
    )
  }
}

export default Profile;