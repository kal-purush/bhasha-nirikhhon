import React, { Component } from "react"
import "./App.css"

import levelOne from "./levels/01"
import levelTwo from "./levels/02"
import Puzzle from "./puzzle"

class Game extends Component {
  state = {}
  constructor(props) {
    super(props)
    this.puzzle = new Puzzle(props.puzzle)
  }
  render() {
    const isSolved = this.puzzle.isSolved()
    console.log(this.puzzle, isSolved)
    return (
      <section
        className="puzzle"
        style={{ borderColor: isSolved ? "green" : "red" }}
      >
        {this.props.render({
          ...this.puzzle,
          onAddPiece: piece => {
            this.puzzle.add(piece, this.state.currentParent)
            this.setState({ currentParent: piece })
          },
          onReset: () => {
            this.puzzle.reset()
            this.setState({ currentParent: null })
          }
        })}
      </section>
    )
  }
}

const Inputs = ({ inputs, expected }) => (
  <ul>
    <li><code>{JSON.stringify(inputs)}</code></li>
    <li><code>{JSON.stringify(expected)}</code></li>
  </ul>
)
const Expected = ({ expected }) => (
  <ul>
    <code>{JSON.stringify(expected)}</code>
  </ul>
)
const Result = ({ result }) => (
  <ul>
    <li>{JSON.stringify(result)}</li>
  </ul>
)
const Node = ({ data, inputs = [] }) => (
  <li>
    <code>{data || "..."}</code>
    <ul>{inputs.map(node => <Node key={node.data} {...node} />)}</ul>
  </li>
)
const Solution = ({ root }) => (
  <ul>
    <Node {...root} />
  </ul>
)
const Pieces = ({ pieces, solution, onAddPiece }) => (
  <ul className="tree">
    {pieces.map(piece => (
      <li
        key={piece}
        style={{ display: solution.contains(piece) ? "none" : "table-cell" }}
        onClick={() => onAddPiece(piece)}
      >
        <code>{piece}</code>
      </li>
    ))}
  </ul>
)
const Actions = ({ onReset }) => (
  <div className="actions">
    <button onClick={onReset}>Reset</button>
  </div>
)

class App extends Component {
  render() {
    return (
      <Game
        puzzle={levelOne}
        render={({
          inputs,
          expected,
          solution,
          result,
          pieces,
          onAddPiece,
          onReset
        }) => {
          return [
            <ul className="tree">
              <Inputs key="inputs" inputs={inputs} expected={expected} />
              <Solution key="solution" {...solution} />
              <Result key="result" result={result} />
            </ul>,
            <Pieces
              key="pieces"
              pieces={pieces}
              solution={solution}
              onAddPiece={onAddPiece}
            />,
            <Actions key="actions" onReset={onReset} />
          ]
        }}
      />
    )
  }
}

export default App