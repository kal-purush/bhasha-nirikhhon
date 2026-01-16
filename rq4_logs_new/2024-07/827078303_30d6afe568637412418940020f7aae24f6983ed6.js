let questions = [
    {
        question: "What is the capital of France?",
        options: ["Paris", "London", "Berlin", "Rome"],
        correct: 0
    },
    {
        question: "What is the largest planet in our solar system?",
        options: ["Earth", "Saturn", "Jupiter", "Uranus"],
        correct: 2
    },
    {
        question: "Who is the author of the book 'To Kill a Mockingbird'?",
        options: ["F. Scott Fitzgerald", "Harper Lee", "Jane Austen", "J.K. Rowling"],
        correct: 1
    },
    //Adicionar mais perguntas
];

let currentQuestion = 0;
let score = 0;

$(document).ready(function() {
    displayQuestion();
    $("#next-question").click(nextQuestion);
    $("#play-again").click(playAgain);
});

function displayQuestion() {
    let question = questions[currentQuestion];
    $("#question").text(question.question);
    $("#options").empty();
    for (let i = 0; i < question.options.length; i++) {
        let option = question.options }
    }