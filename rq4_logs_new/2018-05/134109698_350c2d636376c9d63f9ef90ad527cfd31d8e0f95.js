import React, { Component } from "react";

const clientId = "61023dff117c43b4a0f601ee341ed8bd";
const authorizeUrl = `https://accounts.spotify.com/authorize?client_id=${clientId}&response_type=token&redirect_uri=http://localhost:3000&scope=playlist-read-private`;

class App extends Component {
  state = {};
  componentDidMount() {
    const match = window.location.hash.match(/#(?:access_token)=([\S\s]*?)&/);
    if (match && match[1]) {
      fetch("https://api.spotify.com/v1/me/playlists?limit=50", {
        headers: new Headers({
          Authorization: "Bearer " + match[1]
        })
      })
        .then(res => res.json())
        .then(json => {
          this.setState({ data: json });
        });
    }
  }
  render() {
    const data = this.state.data;
    return (
      <div>
        <h1>Playlister</h1>
        {data ? (
          <div>
            <p>
              <i>
                Showing {data.items.length} of {data.total} playlists.
              </i>
            </p>
            {data.items.map(playlist => (
              <div key={playlist.id}>{playlist.name}</div>
            ))}
          </div>
        ) : (
          <a href={authorizeUrl}>Get Access Token</a>
        )}
      </div>
    );
  }
}

export default App;