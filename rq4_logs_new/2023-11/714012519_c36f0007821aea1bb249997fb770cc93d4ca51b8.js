import { useEffect, useState } from "react";
import "./styles.css";

export default function App() {
  const [lot] = useState([1, 2, 3, 4, 5]);

  const [activeIndex, setActiveIndex] = useState(-1);
  const [isToggling, setIsToggling] = useState(false);
  const [isFinished, setIsFinished] = useState(false);
  const [isRight, setIsRight] = useState(false);
  const [showComponent, setShowComponent] = useState(false);

  const pics = [
    "https://images.fineartamerica.com/images/artworkimages/mediumlarge/2/emoji-squint-small-smile-ali-lynne.jpg",
    "https://encrypted-tbn0.gstatic.com/images?q=tbn:ANd9GcR8gKV8iOSJMmlYlVcrbuQQxjgGwd0JmHZapMQr1EmQIEugGHIrtMbP6bM9tp0K2udMZek&usqp=CAU",
    "https://image.pngaaa.com/909/82909-small.png",
    "https://c8.alamy.com/comp/BMB5W2/illustration-of-a-cool-emoticon-BMB5W2.jpg",
    "https://m.media-amazon.com/images/I/61ZCZE5+ZVL._AC_UF894,1000_QL80_.jpg"
  ];

  const Task = () => {
    return (
      <>
        <p className="task">Click the number, where this smile was:</p>
        <img className="task__img" src={pics[1]} alt="pic" />
      </>
    );
  };

  const RightAnswer = () => (
    <p className="final right">Congrats! It's the right answer!</p>
  );

  const WrongAnswer = () => <p className="final wrong">Nope! Try again</p>;

  const placeAnswer = () => {
    return <>{isRight ? <RightAnswer /> : <WrongAnswer />}</>;
  };

  const toggleClass = () => {
    setActiveIndex((prevIndex) => (prevIndex + 1) % lot.length);

    if (activeIndex === lot.length - 1) {
      stopToggleClass();
      setActiveIndex(-1);
      setIsFinished(true);
    }
  };

  const startToggling = () => {
    setIsToggling(true);
  };

  const stopToggleClass = () => {
    setIsToggling(false);
  };

  const resetGame = () => {
    setActiveIndex(-1);
    setIsToggling(false);
    setIsFinished(false);
    setIsRight(false);
    setShowComponent(false);
  };

  useEffect(() => {
    let interval;
    if (isToggling) {
      interval = setInterval(() => {
        toggleClass();
      }, 700);
    }

    return () => clearInterval(interval);
  }, [isToggling, activeIndex]);

  const checkClick = (e, id) => {
    if (id === 1) {
      setIsRight(true);
      setShowComponent(true);
      placeAnswer();
    } else {
      setIsRight(false);
      setShowComponent(true);
      placeAnswer();
    }
  };

  const lotBuild = lot.map((item, id) => {
    return (
      <div
        key={id}
        className={`squares ${id === activeIndex ? "active" : ""} 
          ${isFinished ? "shake" : ""} ${
          isRight && id === 1 ? "right-answer active" : ""
        }`}
        onClick={(e) => checkClick(e, id)}
      >
        {id === activeIndex ? <img src={pics[id]} alt="pic" /> : item}
      </div>
    );
  });

  return (
    <>
      <div className="App">{lotBuild}</div>
      <button onClick={startToggling}>Play</button>
      <button onClick={resetGame}>Reset</button>
      {isFinished ? <Task /> : null}
      {showComponent && placeAnswer()}
    </>
  );
}