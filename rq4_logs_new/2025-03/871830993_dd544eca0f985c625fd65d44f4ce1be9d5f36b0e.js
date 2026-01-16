let currentQuestion = 1;
let score = 0;
let answered = false;

function displayQuestion() {
	const questionEl = document.getElementById('question');
	const answersEl = document.getElementById('answers');

	answersEl.innerHTML = '';
	answered = false;

	if (currentQuestion === 1) {
                questionEl.textContent = "What is light pollution?";
                addAnswer("Excessive artificial light at night", true);
                addAnswer("Natural light from the sun", false);
                addAnswer("Lack of light in urban areas", false);
                addAnswer("Only light from buildings", false);
	} else if (currentQuestion === 2) {
		questionEl.textContent = "Which of the following is a consequence of light pollution?";
                addAnswer("Better visibility of stars", false);
                addAnswer("Disruption of ecosystems", true);
                addAnswer("Improved public safety", false);
                addAnswer("Reduced electricity usage", false);
	} else if (currentQuestion === 3) {
                questionEl.textContent = "Which source contributes the most to light pollution in urban areas?";
                addAnswer("Billboards and advertisements", false);
                addAnswer("Streetlights and vehicle lights", true);
                addAnswer("Personal home lighting", false);
                addAnswer("Phone screens", false);
	} else if (currentQuestion === 4) {
                questionEl.textContent = "What can individuals do to reduce light pollution?";
                addAnswer("Use energy-efficient outdoor lights", true);
                addAnswer("Keep all lights on overnight", false);
                addAnswer("Increase streetlight brightness", false);
                addAnswer("Install more billboards", false);
	} else if (currentQuestion === 5) {
                questionEl.textContent = "Which of these animals is most affected by light pollution?";
                addAnswer("Bats", true);
                addAnswer("Elephants", false);
                addAnswer("Fish", false);
                addAnswer("Lions", false);
	} else {
                showResult();
	}
}

function addAnswer(text, isCorrect) {
	const answersEl = document.getElementById('answers');
	const button = document.createElement('button');
	button.textContent = text;
	button.className = 'answer';
	button.onclick = () => checkAnswer(button, isCorrect);
	answersEl.appendChild(button);
        }

function checkAnswer(button, isCorrect) {
	if (answered) return;
	answered = true;

	const allButtons = document.querySelectorAll('.answer');
	allButtons.forEach(btn => {
		btn.onclick = null;
                if (btn.textContent.includes('Excessive artificial light at night') ||
                    	btn.textContent.includes('Disruption of ecosystems') ||
                    	btn.textContent.includes('Streetlights and vehicle lights') ||
                    	btn.textContent.includes('Use energy-efficient outdoor lights') ||
                    	btn.textContent.includes('Bats')) {
                    	btn.classList.add('correct');
                } else {
                    	btn.classList.add('incorrect');
                }
            });

	if (isCorrect) {
		score++;
                alert('Correct!');
	} else {
                alert('Incorrect!');
            }
        }

function nextQuestion() {
	if (!answered) {
		alert('Please answer the question first.');
                return;
	}
	currentQuestion++;
	if (currentQuestion <= 5) {
                displayQuestion();
	} else {
                showResult();
            }
	}

function showResult() {
	document.getElementById('quiz-container').classList.add('hidden');
	document.getElementById('result').classList.remove('hidden');
	document.getElementById('score').textContent = score;
        }

function tryAgain() {
	currentQuestion = 1;
	score = 0;
	document.getElementById('quiz-container').classList.remove('hidden');
	document.getElementById('result').classList.add('hidden');
	displayQuestion();
        }

window.onload = displayQuestion;