import React, { Component } from "react";

export default class PlayListTrack extends Component {
  render() {
    const track = this.props.data;
    return (
      <li>
        <div className="mb-4">
          <h4>{track.name}</h4>
          <p>{`from ${track.album.name} ${track.artist.name}`}</p>
        </div>
      </li>
    );
  }
}