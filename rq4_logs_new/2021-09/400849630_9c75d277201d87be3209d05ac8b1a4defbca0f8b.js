import './App.css';
import React, { useState, useEffect } from 'react';



/* 
-Section 2 functional children components that return elements.
<> and </> tags in JSX correspond to <React.Fragment> to not introduce any new HTML elements.
*/
const StarsDisplay = props => (
  <>                                             
    {utils.range(1, props.count).map(starId => ( //create range from 1-9, map the range into array of stars.
      <div key={starId} className="star" />      //assign key attribute to list items of stars.   
    ))} 
  </>
);

const PlayNumber = props => (
  <button
    className="number"
    style={{backgroundColor: colors[props.status]}} //button color based on status
    onClick={() => props.onClick(props.number, props.status)}
  >
    {props.number}
  </button>
);

const PlayAgain = props => (
	<div className="game-done">
  	<div 
    	className="message"
      style={{ color: props.gameStatus === 'lost' ? 'red' : 'green'}} //Ternary operators
    >
  	  {props.gameStatus === 'lost' ? 'Game Over' : 'Calculations indicate you are a genius.'}
  	</div>
	  <button onClick={props.onClick}>Play Again</button>
	</div>
);



//Section 1: Parent Component that calls the Game variable
/*
-When PlayAgain button is pressed gameID is incremented 1.
-React will unmount the game with previous gameId key and mount new game with current gameId key.
-this clears side effects and re-initialize brand new state.
*/ 
const StarMatch = () => {
	const [gameId, setGameId] = useState(1);                                //Initialize game 1
	return <Game key={gameId} startNewGame={() => setGameId(gameId + 1)}/>; //Increment game to new value when PlayAgain button is clicked.
}
/*
  -useGameState is a custom hook used to manage initial state, useffect, and how to set state.   	
  -initialize stars state using utils.random value 1-9 to display random number stars.
  -initialize availableNums state using utils.random value 1-9 to display range 1-9 numbers. 
  -initialize candidateNums state with an empty array.
*/
//initial state
const useGameState = timeLimit => {	
  const [stars, setStars] = useState(utils.random(1, 9));     
  const [availableNums, setAvailableNums] = useState(utils.range(1, 9));
  const [candidateNums, setCandidateNums] = useState([]);
  const [secondsLeft, setSecondsLeft] = useState(30);
  
//side effects  
  useEffect(() => {                                                            //sideffect runs after first render and after every update.
    if (secondsLeft > 0 && availableNums.length > 0) {
      const timerId = setTimeout(() => setSecondsLeft(secondsLeft - 1), 1000); //decrement secondsLeft state by 1 second.
      return () => clearTimeout(timerId);                                      //sideffect clean up mechanism.    
    }
  });
  //how to set state 
  const setGameState = (newCandidateNums) => {
    if (utils.sum(newCandidateNums) !== stars) {   //if newCandidateNums count does not equal count of stars.
			setCandidateNums(newCandidateNums);    //setCandidateNum will update CandidateNums state.   
    } else {                                       //else sum of newcandidateNums == count of stars (Correct Pick!!)    
      const newAvailableNums = availableNums.filter(
        n => !newCandidateNums.includes(n)         //filter non newCandidateNums to be removed from availableNums.  
      );
      setStars(utils.randomSumIn(newAvailableNums, 9)); //redraw number of stars available.
      setAvailableNums(newAvailableNums);               //update state of AvailableNums based on newAvailableNums.
      setCandidateNums([]);                             //update state of CandidateNums to empty array.
    }
  };

  return { stars, availableNums, candidateNums, secondsLeft, setGameState };
};

//Section 3: Variables for computation
const Game = props => {
  const {
    stars,
    availableNums,
    candidateNums,
    secondsLeft,
    setGameState,
  } = useGameState();

  const candidatesAreWrong = utils.sum(candidateNums) > stars; //candidate count number sum > count of stars. 
  const gameStatus = availableNums.length === 0                //is condition 0 available numbers to select?   
  	? 'won'                                                    //true, gameStatus is 'won' 
    : secondsLeft === 0 ? 'lost' : 'active'                    //false, then check if seconds Left is 0?   
																  //0 true, gameStatus is 'lost' : 0 false, gameStatus is 'active'     
  const numberStatus = number => {                             
    if (!availableNums.includes(number)) {                  //if number is not included in available numbers array return 'used'.
      return 'used';
    }

    if (candidateNums.includes(number)) {                   //if number is included in candidate numbers array then
      return candidatesAreWrong ? 'wrong' : 'candidate';       //if true return 'wrong' : false return 'candidate'.
    } 

    return 'available';                                     //otherwise if number is not used or not a candidate return 'available'. 
  };

  const onNumberClick = (number, currentStatus) => {        //if gameStus is not 'active' OR if currentStatus is 'used' 
    if (currentStatus === 'used' || secondsLeft === 0) {    //just return (don't let user do anything)  
      return;
    }
    //We need a fresh newCandidateNums array upon each time user selects a number button
    const newCandidateNums =                                
      currentStatus === 'available'                         //is currentStatus 'available?'
        ? candidateNums.concat(number)                      //true, then concat the number to add to candidateNums array.
        : candidateNums.filter(cn => cn !== number);        //false, filter out number that was clicked by user.

    setGameState(newCandidateNums);
  };
  
  //Section 3 the Return section which renders elements to display the UI based on state and variables
  return (
    <div className="game">
      <div className="help">
        Pick 1 or more numbers that sum to the number of stars
      </div>
      <div className="body">
        <div className="left">
          {gameStatus !== 'active' ? (                      //is the gameStatus not active?   
          	<PlayAgain onClick={props.startNewGame}         //true, call the PlayAgain and resetGame components. 
			gameStatus={gameStatus} />                      //and update gameStatus to display if game 'won' or 'lost'.
          ) : (
          	<StarsDisplay count={stars} />                  //false, call the StarsDisplay component.   
          )}
        </div>
        <div className="right">
          {utils.range(1, 9).map(number => (
            <PlayNumber
              key={number}
              status={numberStatus(number)}
              number={number}
              onClick={onNumberClick}
            />
          ))}
        </div>
      </div>
      <div className="timer">Time Remaining: {secondsLeft}</div>
    </div>
  );
};



// Color Theme
const colors = {
  available: 'lightgray',
  used: 'lightgreen',
  wrong: 'lightcoral',
  candidate: 'deepskyblue',
};

// Math science
const utils = {
  // Sum an array using reduce function to return sum of all elements of an array

  sum: arr => arr.reduce((acc, curr) => acc + curr, 0),

  // create an array of numbers between min and max (edges included)
  range: (min, max) => Array.from({length: max - min + 1}, (_, i) => min + i),

  // pick a random number between min and max (edges included)
  random: (min, max) => min + Math.floor(Math.random() * (max - min + 1)),

  // Given an array of numbers and a max...
  // Pick a random sum (< max) from the set of all available sums in arr
  randomSumIn: (arr, max) => {
    const sets = [[]];
    const sums = [];
    for (let i = 0; i < arr.length; i++) {
      for (let j = 0, len = sets.length; j < len; j++) {
        const candidateSet = sets[j].concat(arr[i]);
        const candidateSum = utils.sum(candidateSet);
        if (candidateSum <= max) {
          sets.push(candidateSet);
          sums.push(candidateSum);
        }
      }
    }
    return sums[utils.random(0, sums.length - 1)];
  },
};

export default StarMatch;