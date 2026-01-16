import React from "react";
import "./app.css";
import Dropdown from "./Dropdown";
import GameWinner from "./GameWinner";

const winningOptions = [
  [0, 1, 2],
  [3, 4, 5],
  [6, 7, 8],
  [0, 3, 6],
  [1, 4, 7],
  [2, 5, 8],
  [0, 4, 8],
  [2, 4, 6],
];
const phraseArray=[
  'წადით წადით დაიძინეთ...',
  'არ გცოდნიათ ბიძია-ბაბუა თქვენ თამაში',
  'მასაჟი თუ მასაზი?',
  'ნიჩიაა...',
  'ანდრია რამე ხო არ გეშლება?',
  'ალაქაჩი და სოლოსოლო თქვენზე უკეთ თამაშობენ',
  'ბავშვები თამანსობენ',
  'მაღალი... მაღალი...',
  'ესევ ჰალილო ათამაშო ჯობია ანდრი :)',
  'გეყოთ ეხლა ამდენი თამაში ხო ხედავთ გარტყმაში არ ხართ',
  'მორჩა კინო',
  'აბა თქვენა ხართ რაა..',
  'ჩემი კაი გავგიჟდი კომპიუტერი კაცი',
  'ალიბაბა და 12 ყივჩაღი',
  'ასტანა ვესტა ბეიბე',
  'აიტ მიაფსი მამაშენს',
  'ავათა ვარ ავათააა მოდი სანახავადაა, გავიბაგე ჩემი კაი...'
]
const options = [
  { value: "X", label: "X" },
  { value: "O", label: "O" },
];
function App() {
  const boardRef = React.useRef();
  const [currentTurn, setCurrentTurn] = React.useState();
  const [boardArray, {}] = React.useState([1, 2, 3, 4, 5, 6, 7, 8, 9]);
  const [winner, setWinner] = React.useState({ state: false, status: "" });
  const [player1, setPlayer1] = React.useState();
  const [player2, setPlayer2] = React.useState();
  const [result, setResult] = React.useState({ player1: 0, player2: 0 });
  const [qty, setQty] = React.useState(1);

  async function handleClick(e) {
    if (
      !e.target.classList.contains("x") &&
      !e.target.classList.contains("o")
    ) {
      if (currentTurn === 0) {
        await e.target.classList.add("o");
        if (geTwinner("o")) return;
        if (draw()) return;
        setCurrentTurn(1);
      } else {
        await e.target.classList.add("x");
        if (geTwinner("x")) return;
        else {
          if (draw()) return;
          setCurrentTurn(0);
        }
      }
    }
    return;
  }

  function geTwinner(currentTurn) {
    const boardArr = boardRef?.current.children;
    for (let i of winningOptions) {
      let filledBoxCounter = []; //to have an access on filled indexes in order to get success color
      for (let j = 0; j < i.length; j++) {
        if (boardArr[i[j]].classList.contains(currentTurn)) {
          filledBoxCounter.push(i[j]);
        }
        if (filledBoxCounter.length === 3) {
          for (let i of filledBoxCounter) {
            boardArr[i].classList.add("green");
          }
          setWinner({ state: true, status: currentTurn });
          currentTurn === "x"
            ? setResult({
                player1: result.player1 + 1,
                player2: result.player2,
              })
            : setResult({
                player1: result.player1,
                player2: result.player2 + 1,
              });
          return true;
        }
      }
    }
    return false;
  }
  function nextSet() {
    const boardArr = boardRef?.current.children;
    setWinner({ state: false, status: "" });
    for (let i of boardArr) {
      if (i.classList.contains("x")) {
        i.classList.remove("x");
      }
      if (i.classList.contains("green")) {
        i.classList.remove("green");
      }
      if (i.classList.contains("o")) {
        i.classList.remove("o");
      }
    }
  }
  function restart() {
    const boardArr = boardRef?.current.children;
    setCurrentTurn();
    setPlayer1();
    setQty(1)
    setPlayer2();
    setResult({ player1: 0, player2: 0 });
    setWinner({ state: false, status: "" });
    for (let i of boardArr) {
      if (i.classList.contains("x")) {
        i.classList.remove("x");
      }
      if (i.classList.contains("green")) {
        i.classList.remove("green");
      }
      if (i.classList.contains("o")) {
        i.classList.remove("o");
      }
    }
  }
  function draw() {
    let drawCounter = 0;
    const boardArr = boardRef?.current.children;
    for (let i of boardArr) {
      if (i.classList.length === 0) {
        return false;
      } else {
        drawCounter++;
      }
    }
    if (drawCounter === 9) {
      setWinner({ state: true, status: "draw" });
      return true;
    }
  }

  function getWinnerStatus(gameWinner) {
    switch (winner.status) {
      case "x":
        return gameWinner ? "X" : "Winner is Player - X ზედმეტსახელად ალაქაჩი";
      case "o":
        return gameWinner ? 'O' : "Winner is Player - O ზედმეტსახელად ჰალილო";
      default:
        return phraseArray[Math.floor(Math.random() * phraseArray.length)];
    }
  }

  function handleChange(val, player) {
    setCurrentTurn(val.value === "X" ? 1 : 0);
    let tmp = Object.assign(options);
    let item = tmp.filter((item) => item.value !== val.value);
    if (player === 1) {
      setPlayer1(val);
      setPlayer2(item[0]);
    } else {
      setPlayer2(val);
      setPlayer1(item[0]);
    }
  }
  function preventGame() {
    if (winner.state || !player1 || !player2) {
      return true;
    }
    return false;
  }

  function getColor(x, y) {
    if (x > y) {
      return "#00ff0a";
    } else if (y > x) {
      return "red";
    } else {
      return "#f2f2f2";
    }
  }
  function checkTurn() {
    if (currentTurn === 0) {
      return "O";
    } else if (currentTurn === 1) {
      return "X";
    } else {
      return "Game not startet yet";
    }
  }
  return (
    <div id="board_wrapper">
    
      <div>
        <div>
          <span style={{ color: "#f2f2f2" }}>Player - X: </span>
          <span style={{ color: getColor(result.player1, result.player2) }}>
            {result.player1}
          </span>
        </div>
        <div>
          <span style={{ color: "#f2f2f2" }}>Player - O: </span>
          <span style={{ color: getColor(result.player2, result.player1) }}>
            {result.player2}
          </span>
        </div>
      </div>
      <div className="brd">
        <div className="board" ref={boardRef}>
          {boardArray.map((item, index) => (
            <div
              key={index}
              onClick={(e) => !preventGame() && handleClick(e)}
            ></div>
          ))}
        </div>
        <div style={{ display: "flex", flexDirection: "column" }}>
          <span
            style={{
              color: "#fff",
              fontSize: "20px",
              marginTop: "10px",
              textAlign: "center",
            }}
          >
            Current Turn - {checkTurn()}
          </span>
          <button className="btn" onClick={restart}>
            Restart
          </button>
        </div>
      </div>
      <div>
        <div style={{ width: "200px" }}>
          <span
            style={{
              color: "#f2f2f2",
            }}
          >
            Player 1
          </span>
          <Dropdown
            onChange={(val) => handleChange(val, 1)}
            options={options}
            value={player1}
            disabled={currentTurn !== undefined && true}
          />
        </div>
        <div style={{ margin: "15px 0" }}>
          <span
            style={{
              color: "#f2f2f2",
            }}
          >
            Player 2
          </span>
          <Dropdown
            options={options}
            onChange={(val) => handleChange(val, 2)}
            value={player2}
            disabled={currentTurn !== undefined && true}
          />
        </div>
        <div>
          <input
            type="number"
            min={1}
            placeholder="სადამდე თამაშობთ?"
            value={qty}
            onChange={(e) => e.target.value!== 0 &&  setQty(e.target.value)}
          />
        </div>
      </div>
      {winner.state && (
     
        <div className="modal">
          { result.player1  === Number(qty) || result.player2 === Number(qty)  ? 
          <GameWinner restart={restart} result={result} getWinnerStatus={getWinnerStatus}/> :
          <>
          <span>{getWinnerStatus()}</span>
          <button onClick={nextSet}>Next set</button>
          <button onClick={restart}>Restart</button>
          </>
          }
        </div>
      )}
  
    </div>
  );
}

export default App;