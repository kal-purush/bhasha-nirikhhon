import "./styles.css";
import { useState } from "react";

const emojiDictionary = {
  "😀": "Smiling face",
  "😆": "Grinning face",
  "😂": "Lauging with tears",
  "🙃": "Upside down face",
  "😉": "Winking face",
  "😇": "Smiling face with halo",
  "😎": "Smiling face with sunglasses",
  "🤓": "Nerdy face",
  "🥳": "Partying face",
  "🥰": "Smiling face with hearts",
  "😐": "Neutral face",
  "🤨": "Face with raised eyebrow",
  "😏": "	Smirking face",
  "😒": "	Unamused face",
  "😔": "Pensive face",
  "😷": "Face with mask",
  "😟": "Worried face",
  "😰": "Anxious face with sweat",
  "🤡": "	Clown",
  "👻": "	Ghost"
};

var emojiArray = Object.keys(emojiDictionary);
export default function App() {
  var [meaning, setMeaning] = useState("");

  function inputClickHandler(event) {
    var inputValue = event.target.value;
    if (inputValue in emojiDictionary) {
      meaning = emojiDictionary[inputValue];
      setMeaning(meaning);
    } else if (inputValue === "") {
      setMeaning("");
    } else {
      setMeaning("Emoji not in dictionary");
    }
  }

  function clearHandler() {
    setMeaning("");
    var p = document.querySelector(".userInput");
    p.value = "";
  }

  function emojiClickHandler(item) {
    meaning = emojiDictionary[item];
    setMeaning(meaning);
    var p = document.querySelector(".userInput");
    p.value = item;
  }

  return (
    <div className="App">
      <h1>Emoji Interpreter</h1>
      <input className="userInput" onChange={inputClickHandler} />
      <div style={{ padding: "0.5rem", fontWeight: "bold", color: "blue" }}>
        {meaning}
      </div>
      <button className="clearButton" onClick={clearHandler}>
        Clear
      </button>
      <div style={{ fontWeight: "bold" }}>
        <h3>Emojis We know</h3>
      </div>
      <div className="emojiList">
        {emojiArray.map((item) => {
          return (
            <span
              style={{ cursor: "pointer" }}
              onClick={() => emojiClickHandler(item)}
              key={item}
            >
              {item}
            </span>
          );
        })}
      </div>
    </div>
  );
}