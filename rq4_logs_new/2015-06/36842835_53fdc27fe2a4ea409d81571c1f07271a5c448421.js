import React from 'react';
import Router from 'react-router';

class SpotifySearch extends React.AutoBindComponent {
  
  constructor(props){
    super(props);
    this.state = { value: '' };

    this.submit = this.submit.bind(this);
    this.handleChange = this.handleChange.bind(this);
  }

  submit(e) {
    e.preventDefault();

    console.log('Submit passed', this.state.value);
  }

  handleChange(e) {
    this.setState({value: e.target.value});
  }

  render() {
    return (
      <form className="form-inline container" onSubmit={ this.submit }>
        <div className="center-block ">
          <div className="form-group">
            <input className="form-control" type="search" id="searchTxt" value={this.state.value} onChange={this.handleChange} placeholder="Search Spotify" />
          </div>
           <button type="submit" className="btn btn-default">Search</button>
        </div>    
      </form>
    );
  }
                                     
}

export default SpotifySearch;