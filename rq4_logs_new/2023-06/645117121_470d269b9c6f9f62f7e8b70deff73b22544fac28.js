import { useState } from "react";
import { Route, Routes, BrowserRouter } from "react-router-dom";
// import { Link } from "react-router-dom";
import "./App.css";
import Board from "./components/Board/Board";
import ScoreBoard from "./components/ScoreBoard/ScoreBoard";
import Landing from "./components/Landing/Landing";

const winningPosition = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8],
  [0, 3, 6],
  [1, 4, 7],
  [2, 5, 8],
  [0, 4, 8],
  [2, 4, 6],
];

const App = () => {
  
  const [turn, setTurn] = useState("X");
  const [squares, setSquares] = useState(Array(9).fill(null));
  const [winningSquares, setWinningSquare] = useState([]);
  const [score, setScore] = useState({
    X: 0,
    O: 0,
  });

  const reset = () => {
    setTurn("X");
    setSquares(Array(9).fill(null));
    setWinningSquare([]);
  };

  const checkForWinner = (newSquares) => {
    for (let i = 0; i < winningPosition.length; i++) {
      const [a, b, c] = winningPosition[i];
      if (
        newSquares[a] &&
        newSquares[a] === newSquares[b] &&
        newSquares[a] === newSquares[c]
      ) {
        //existe un ganador
        endGame(newSquares[a], winningPosition[i]);
        return;
      }
    }
    if (!newSquares.includes(null)) {
      //existe  un empate
      endGame(null, Array.from(Array(10).keys()));
      return;
    }
    setTurn(turn === "X" ? "O" : "X");
  };

  const handleClick = (square) => {
    let newSquare = [...squares];
    newSquare.splice(square, 1, turn);
    setSquares(newSquare);
    checkForWinner(newSquare);
  };

  const endGame = (result, winningPosition) => {
    setTurn(null);
    if (result !== null) {
      setScore({
        ...score,
        [result]: score[result] + 1,
      });
    }
    setWinningSquare(winningPosition);
    setTimeout(reset, 2000);
  };

  return (
    <BrowserRouter>
      <div className="container">
        <Routes>
          <Route path="*" element={<Landing />} />
          <Route
            exact
            path="/home"
            element={
              <>
                <Board
                  winningSquares={winningSquares}
                  turn={turn}
                  squares={squares}
                  onClick={handleClick}
                />
                <ScoreBoard scoreO={score.O} scoreX={score.X} />
              </>
            }
          />
        </Routes>
      </div>
    </BrowserRouter>
  );
};

export default App;