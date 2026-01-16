import React, { useEffect, useState } from 'react';
import axios from 'axios';
import { shuffleArray } from '../shuffleArray';
import Board from './Board';
import Header from './Header';
import Route from './Route';
import Modal from './Modal';
import Instruction from './Instruction';

const App = () => {
  const [characters, setCharacterData] = useState([]);
  const [maxScore, setMaxScore] = useState(null);
  const [bestScore, setBestScore] = useState(0);
  const [currentScore, setCurrentScore] = useState(0);
  const [gameOver, setGameOver] = useState(false);

  useEffect(() => {
    const fetchData = async () => {
      try {
        const { data } = await axios.get(
          'https://rickandmortyapi.com/api/character/1,2,3,4,5,7,343,118,47,180,115,103,242,331,558,208,564,375,107,171'
        );

        initData(data);
        setCharacterData(shuffleArray(data));
        setMaxScore(data.length);
      } catch (err) {
        console.error(err.message);
      }
    };
    fetchData();
  }, []);

  useEffect(() => {
    setCharacterData(shuffleArray(characters));
    if (currentScore > bestScore) setBestScore(currentScore);
    if (currentScore === maxScore) setGameOver(true);
  }, [currentScore]);

  const handleClick = char => {
    if (!char.isClicked) {
      setCharacterData(
        characters.map(character =>
          character.id === char.id
            ? { ...character, isClicked: true }
            : character
        )
      );
      setCurrentScore(currentScore + 1);
    }

    if (char.isClicked) {
      resetGame();
    }
  };

  const initData = data => {
    data.forEach(character => (character.isClicked = false));
  };

  const resetGame = () => {
    initData(characters);
    setCurrentScore(0);
    setGameOver(false);
  };

  return (
    <>
      <Header bestScore={bestScore} />
      <p className="current-score">Current Score: {currentScore}</p>
      <Route path="/">
        <Board
          characters={characters}
          handleClick={handleClick}
          currentScore={currentScore}
        />
      </Route>
      <Route path="/instruction">
        <Instruction />
      </Route>
      <Modal gameOver={gameOver} resetGame={resetGame} />
    </>
  );
};

export default App;