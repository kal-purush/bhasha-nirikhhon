import React from 'react';
import './index.css';

class Character extends React.Component {
  state = {
    characters: [],
  }

  componentDidMount() {
    this.fetchCharacter();
  }

  async fetchCharacter() {
    const url = 'https://gateway.marvel.com:443/v1/public/characters/1009664?apikey=42b5095143efb46a0176cbb70e28b104&ts=0&hash=0441ba8faeb7dfffb305b57ef0596500';
    const response = await fetch(url);
    const data = await response.json();
    this.setState({ characters: data.data.results});
  }



  render() {
    return (
      <div className="Character">
          <p>as</p>
      </div>
    );
  }

}

export default Character;