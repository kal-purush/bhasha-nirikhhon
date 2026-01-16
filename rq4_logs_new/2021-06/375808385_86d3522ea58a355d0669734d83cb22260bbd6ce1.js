import React, { useState } from "react";

const App = () => {
  const anecdotes = [
    "If it hurts, do it more often",
    "Adding manpower to a late software project makes it later!",
    "The first 90 percent of the code accounts for the first 90 percent of the development time...The remaining 10 percent of the code accounts for the other 90 percent of the development time.",
    "Any fool can write code that a computer can understand. Good programmers write code that humans can understand.",
    "Premature optimization is the root of all evil.",
    "Debugging is twice as hard as writing the code in the first place. Therefore, if you write the code as cleverly as possible, you are, by definition, not smart enough to debug it.",
    "Programming without an extremely heavy use of console.log is same as if a doctor would refuse to use x-rays or blod tests when dianosing patients",
  ];

  const [selected, setSelected] = useState(0);
  const [voted, setVoted] = useState(false);
  const [votes, setVotes] = useState(new Uint8Array(anecdotes.length));
  var maxVotes = Math.max(...votes);

  const next = () => {
    let choice = Math.floor(Math.random() * anecdotes.length);
    setSelected(choice); //on clicking next a new random integer is chosen as index for next anecdote in array
  };

  const voteClicked = () => {
    maxVotes = Math.max(...votes);
    const newVotes = [...votes];
    newVotes[selected] += 1;
    setVotes(newVotes);
    setVoted(true); // recording that a vote has been cast to show most voted ancedote
  };

  return (
    <div>
      <h1>Anecdote of the day</h1>
      {anecdotes[selected]}
      <p>has {votes[selected]} votes</p>
      <button onClick={voteClicked}>Vote</button>
      <button onClick={next}>Next Anecdote</button>

      {voted && (
        <>
          <h1>Anecdote with the most votes</h1>
          {anecdotes[votes.indexOf(maxVotes)]}
          <p> has {votes[votes.indexOf(maxVotes)]} votes </p>
        </> // found index of element with the max votes and displayed it's votes and anecdote
      )}
    </div>
  );
};

export default App;