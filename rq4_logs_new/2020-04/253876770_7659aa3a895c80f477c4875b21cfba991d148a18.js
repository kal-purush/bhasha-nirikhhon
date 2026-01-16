import React, { Component } from 'react';


class SlothDetails  extends Component {

  render() {
    const {sloth} = this.props;

    return (
      <div class="card">
        <img class="card-img-top" src="..." alt="Card image cap"/>
        <div class="card-body">
          <h5 class="card-title">{sloth.name}</h5>
          <p class="card-text">Some quick example text to build on the card title and make up the bulk of the card's content.</p>
          <a href="#" class="btn btn-primary">Go somewhere</a>
        </div>
      </div>
    );
  }
}

export default SlothDetails;