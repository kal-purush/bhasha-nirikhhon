import React, {useState} from 'react';
import ReactDOM from 'react-dom';
import './index.css';

const Button = ({name, onClick}) => <button onClick={onClick}>{name}</button>


const App = (props) => {

  const numAnecdotes = props.anecdotes.length

  const [selected, setSelected] = useState(Math.floor(Math.random()*numAnecdotes))
  const [votes, setVotes] = useState(Array.apply(null, new Array(numAnecdotes)).map(Number.prototype.valueOf,0))

  const indexOfMostVotedAnecdote = votes.indexOf(Math.max(...votes))

  const handleNext = () => setSelected(Math.floor(Math.random()*numAnecdotes))

  const handleVote = () => {
    const newVotes = [...votes]
    newVotes[selected]+=1
    setVotes(newVotes)

  }

  return (
    <div>
      <h1> Anecdote of the day </h1>
      <div>
        {props.anecdotes[selected]}
      </div>
      <div>
        has {votes[selected]} votes
      </div>
      <div>
        <Button name='vote' onClick={handleVote}/>
        <Button name='next anecdote' onClick={handleNext}/>
      </div>
      <h1>Anecdote with most votes</h1>
      <div>
        {props.anecdotes[indexOfMostVotedAnecdote]}
      </div>
      <div>
        has {votes[indexOfMostVotedAnecdote]} votes
      </div>
    </div>
  )
}

const anecdotes = [
  'If it hurts, do it more often',
  'Adding manpower to a late software project makes it later!',
  'The first 90 percent of the code accounts for the first 90 percent of the development time...The remaining 10 percent of the code accounts for the other 90 percent of the development time.',
  'Any fool can write code that a computer can understand. Good programmers write code that humans can understand.',
  'Premature optimization is the root of all evil.',
  'Debugging is twice as hard as writing the code in the first place. Therefore, if you write the code as cleverly as possible, you are, by definition, not smart enough to debug it.'
]


ReactDOM.render(<App anecdotes={anecdotes}/>, document.getElementById('root'));

// If you want your app to work offline and load faster, you can change
// unregister() to register() below. Note this comes with some pitfalls.
// Learn more about service workers: https://bit.ly/CRA-PWA