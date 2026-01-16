import React from 'react';

class MovieForm extends React.Component {
  constructor(props) {
    super(props);
    this.state = {
      movieName: '',
      moviePoster: '',
      comment: '',
    };
  }

  onChange = (e) => {
    this.setState({
      [e.target.name]: e.target.value,
    });
  };

  submitForm = (e) => {
    e.preventDefault();
  };

  handleSubmit = (e) => {
    const url = 'https://post-a-form.herokuapp.com/api/movies/';
    const config = {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(this.state),
    };
    fetch(url, config)
      .then((res) => res.json())
      .then((res) => {
        if (res.error) {
          alert(res.error);
        } else {
          alert('Movie added');
        }
      })
      .catch((e) => {
        console.error(e);
        alert('Movie not added.');
      });
  };

  render() {
    return (
      <div className="MovieForm">
        <h1>Your Fav Movie</h1>
        <form onSubmit={this.handleSubmit}>
          <fieldset>
            <legend>Information</legend>
            <div className="form-data">
              <label htmlFor="movieName"> Movie Name </label>
              <input
                type="text"
                id="movieName"
                name="movieName"
                onChange={this.onChange}
                value={this.state.movieName}
              />
            </div>

            <div className="form-data">
              <label htmlFor="moviePoster"> Movie Poster </label>
              <input
                type="text"
                id="moviePoster"
                name="moviePoster"
                onChange={this.onChange}
                value={this.state.moviePoster}
              />
            </div>

            <div className="form-data">
              <label htmlFor="comment">
                Why do you like this movie? What made you stand out? etc.
              </label>
              <textarea
                name="comment"
                id="comment"
                onChange={this.onChange}
                value={this.state.comment}
              />
            </div>
            <hr />
            <div className="form-data">
              <input onClick={this.handleSubmit} type="submit" value="Send" />
            </div>
          </fieldset>
        </form>
      </div>
    );
  }
}

export default MovieForm;