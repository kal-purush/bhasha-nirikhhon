document.addEventListener('DOMContentLoaded', function() {
    document.getElementById('name-form').addEventListener('submit', function(event) {
        event.preventDefault();
        const name = document.getElementById('kid-name').value;
        if (name) {
            document.getElementById('greeting').innerText = 'Well done ' + name + '!';
            document.getElementById('name-input-section').classList.add('d-none');
            document.getElementById('game-section').classList.remove('d-none');
            startGame(name);
        }
    });

    function startGame(name) {
        // Initialize game variables
        let progress = 0;
        const questions = [
            { question: 'What is 2 + 2?', answer: '4' },
            { question: 'What is 3 + 3?', answer: '6' }
        ];
        let currentQuestionIndex = 0;

        function showQuestion() {
            if (currentQuestionIndex < questions.length) {
                const question = questions[currentQuestionIndex];
                document.getElementById('question-section').innerHTML = `
                    <div class="card">
                        <div class="card-body">
                            <h5 class="card-title">${question.question}</h5>
                            <input type="text" class="form-control form-control-lg" id="answer-input" placeholder="Your answer">
                            <button class="btn btn-success btn-lg mt-2" id="submit-answer">Submit</button>
                            <div id="feedback" class="mt-2 text-danger"></div>
                        </div>
                    </div>
                `;
                document.getElementById('submit-answer').addEventListener('click', function() {
                    const answer = document.getElementById('answer-input').value;
                    if (answer === question.answer) {
                        progress += 50; // Assuming 2 questions for simplicity
                        document.getElementById('progress-bar').style.width = progress + '%';
                        currentQuestionIndex++;
                        showQuestion();
                    } else {
                        document.getElementById('feedback').innerText = 'Oops! Try again, you can do it!';
                    }
                });
            } else {
                document.getElementById('question-section').innerHTML = '<h3>Congratulations ' + name + '! You have completed the game!</h3>';
            }
        }

        showQuestion();
    }
});