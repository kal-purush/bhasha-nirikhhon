import React from "react";
import { useState } from "react";
import Result from "../Result/Result";
import './QuestionBox.css'

export default function QuestionBox(props) {
  const { questions } = props;
  const [questionNo, setQuestionNo] = useState(0);
  const [score, setScore] = useState(0);
  const [highlightText, sethighlightText] = useState(false);
  const handleOption = (option) => {
    setQuestionNo(questionNo + 1);
    if (questions[questionNo].options[option].isCorrect) {
      setScore(score + 1);
    }
  };
  return (
    <div className="main-container">
      {questionNo < questions.length ? (
        <div className="question-content">
          <h5>
            Question: {questionNo + 1} of {questions.length}
          </h5> 
          <h3 style={{ color: highlightText ? "red" : "white" }}>
            {questions[questionNo].text}
          </h3>
          <div className="options-pad">
          <button
            onClick={() => {
              handleOption(0);
            }}
          >
            {questions[questionNo].options[0].text}
          </button>
          <button
            onClick={() => {
              handleOption(1);
            }}
          >
            {questions[questionNo].options[1].text}
          </button>
          <button
            onClick={() => {
              handleOption(2);
            }}
          >
            {questions[questionNo].options[2].text}
          </button>
          <button
            onClick={() => {
              handleOption(3);
            }}
          >
            {questions[questionNo].options[3].text}
          </button>
          </div>
          <br />
          <div className="highlight-options">
          <button
            onClick={() => {
              sethighlightText(true);
            }}
          >
            Highlight
          </button>
          <button
            onClick={() => {
              sethighlightText(false);
            }}
          >
            Remove Highlight
          </button>
          </div>
        </div>
      ) : (
        <Result score={score} />
      )}
    </div>
  );
}