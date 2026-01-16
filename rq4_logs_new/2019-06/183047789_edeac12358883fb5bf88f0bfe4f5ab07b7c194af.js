import React, {useState} from 'react';


const StudyWindow = props => {

    const [toSeePile, updateToSee] = useState(props.deck);
    //const [toSeePile, updateToSee] = useState(props.deck.sort(function(a, b){return 0.5 - Math.random()}));
    const [seenPile, updateSeen] = useState([]);
    const [currentAnswer, setCurrentAnswer] = useState([]);
    
    let currentCharacter = toSeePile[0];
    

    const handleTextChange = (event) => {
        console.log(event.target.value);
        setCurrentAnswer(event.target.value.toLowerCase());
    }

    const handleSubmit = () => {
        if(currentAnswer === currentCharacter.romanization){
            console.log("correct!");
            setCurrentAnswer('');
            seenPile.push(toSeePile[0]);
            toSeePile.shift();
        }
        else{
            console.log("false");
            toSeePile.push(toSeePile[0]);
            toSeePile.shift();
        }
    }

    return(
      <div className="study-window">
          <div className="characterDisplay">
              {currentCharacter.character}
          </div>
          <div>
            <input type="text" value={currentAnswer} onChange={handleTextChange}></input>
            <button onClick={handleSubmit}>Submit</button>
          </div>
      </div>
    )
}

export default StudyWindow;