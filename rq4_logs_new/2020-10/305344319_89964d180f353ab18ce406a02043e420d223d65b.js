import React, { useState } from 'react';

export default function App() {
	const questions = [
		{
			questionText: 'What is the largest country in the world',
			answerOptions: [
				{ answerText: 'Sri lanka', isCorrect: false },
				{ answerText: 'Russia', isCorrect: true },
				{ answerText: 'Germany', isCorrect: false},
				{ answerText: 'India', isCorrect: false },
			],
		},
		{
			questionText: 'Who is CEO of apple',
			answerOptions: [
				{ answerText: 'Jeff Bezos', isCorrect: false },
				{ answerText: 'Tim cook', isCorrect: true },
				{ answerText: 'Bill Gates', isCorrect: false },
				{ answerText: 'Tony Stark', isCorrect: false },
			],
		},
		{
			questionText: 'who is the leading company in mobiles',
			answerOptions: [
				{ answerText: 'Apple', isCorrect: true },
				{ answerText: 'Samsung', isCorrect: false },
				{ answerText: 'Sony', isCorrect: false },
				{ answerText: 'One Plus', isCorrect: false },
			],
		},
		{
			questionText: 'Harry Potter has a scar of which shape?',
			answerOptions: [
				{ answerText: 'snake', isCorrect: false },
				{ answerText: 'lightning bolt', isCorrect: true },
				{ answerText: 'tiger', isCorrect: false },
				{ answerText: 'circular', isCorrect: false },
			],

		},
		{
		questionText: 'What is the smallest country in the world',
			answerOptions: [ 
			{ answerText: 'India', isCorrect: false },
			{ answerText: 'Vatican city', isCorrect: true },
			{ answerText: 'Brazil', isCorrect: false },
			{ answerText: 'Japan', isCorrect: false },

			],
		}
		
	];

	const [currentQuestion, setCurrentQuestion] = useState(0);
	const [showScore, setShowScore] = useState(false);
	const [score, setScore] = useState(0);

	const handleAnswerOptionClick = (isCorrect) => {
		if (isCorrect) {
			setScore(score + 1);
		}

		const nextQuestion = currentQuestion + 1;
		if (nextQuestion < questions.length) {
			setCurrentQuestion(nextQuestion);
		} else {
			setShowScore(true);
		}
	};
	return (
		<div className='app'>
			{showScore ? (
				<div className='score-section'>
					You scored {score} out of {questions.length}
				</div>
			) : (
				<>
					<div className='question-section'>
						<div className='question-count'>
							<span>Question {currentQuestion + 1}</span>/{questions.length}
						</div>
						<div className='question-text'>{questions[currentQuestion].questionText}</div>
					</div>
					<div className='answer-section'>
						{questions[currentQuestion].answerOptions.map((answerOption) => (
							<button onClick={() => handleAnswerOptionClick(answerOption.isCorrect)}>{answerOption.answerText}</button>
						))}
					</div>
				</>
			)}
		</div>
	);
}