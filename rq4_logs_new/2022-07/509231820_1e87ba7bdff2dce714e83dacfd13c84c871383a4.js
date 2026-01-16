function submitForm(e) {
    e.preventDefault();
    let cat = document.category_form.categorys.value;
    let level = document.level_form.levels.value;

    //STORING IN SESSION STORAGE
    sessionStorage.setItem("category", cat);
    sessionStorage.setItem("level", level);
    location.href = "index.html";
}

let category = sessionStorage.getItem("category");
let level = sessionStorage.getItem("level");
let score = 0;
let data;
const question = document.querySelector(".question");
const h2 = document.querySelector("h2");
const option1 = document.querySelector("#option1");
const option2 = document.querySelector("#option2");
const option3 = document.querySelector("#option3");
const option4 = document.querySelector("#option4");
const submit = document.querySelector("#submit");
const answers = document.querySelectorAll(".answer");
const showscore = document.querySelector("#showscore");

const categorylink = [
    {
        "science&nature": "https://opentdb.com/api.php?amount=10&category=17&difficulty=easy&type=multiple",
        "gk": "https://opentdb.com/api.php?amount=10&category=9&difficulty=easy&type=multiple",
        "animals": "https://opentdb.com/api.php?amount=10&category=27&difficulty=easy&type=multiple",
        "computers": "https://opentdb.com/api.php?amount=10&category=18&difficulty=easy&type=multiple",
        "history": "https://opentdb.com/api.php?amount=10&category=23&difficulty=easy&type=multiple"
    },
    {
        "science&nature": "https://opentdb.com/api.php?amount=10&category=17&difficulty=medium&type=multiple",
        "gk": "https://opentdb.com/api.php?amount=10&category=9&difficulty=medium&type=multiple",
        "animals": "https://opentdb.com/api.php?amount=10&category=27&difficulty=medium&type=multiple",
        "computers": "https://opentdb.com/api.php?amount=10&category=18&difficulty=medium&type=multiple",
        "history": "https://opentdb.com/api.php?amount=10&category=23&difficulty=medium&type=multiple"
    },
    {
        "science&nature": "https://opentdb.com/api.php?amount=10&category=17&difficulty=hard&type=multiple",
        "gk": "https://opentdb.com/api.php?amount=10&category=9&difficulty=hard&type=multiple",
        "animals": "https://opentdb.com/api.php?amount=10&category=27&difficulty=hard&type=multiple",
        "computers": "https://opentdb.com/api.php?amount=10&category=18&difficulty=hard&type=multiple",
        "history": "https://opentdb.com/api.php?amount=10&category=23&difficulty=hard&type=multiple"
    }]

let questionCount = 0;
const options = [option1, option2, option3, option4]
let correctans;
let rand = -1;
function loadQuestion() {
    if (rand != -1) {
        options[rand].classList.remove("addgreen");
    }
    const result = data.results;
    rand = Math.floor(Math.random() * 4);
    question.innerHTML = result[questionCount].question;
    options[rand].innerHTML = result[questionCount].correct_answer;
    correctans = `ans${rand + 1}`;
    let x = 0;
    for (let i = 0; i <= 3; i++) {
        if (i != rand) {
            options[i].innerHTML = result[questionCount].incorrect_answers[x];
            x++;
        }

    }
}
const getCheckAnswer = () => {
    let answer;
    answers.forEach((curopt) => {
        if (curopt.checked) {
            answer = curopt.id;
        }
    })
    return answer;
}


const deselectAll = () => {
    answers.forEach((curopt) => {
        if (curopt.checked) {
            curopt.checked = false;
        }
    })
}

submit.addEventListener("click", () => {
    options[rand].classList.add("addgreen");
    const checkedAnswer = getCheckAnswer();

    if (checkedAnswer === correctans) {
        score = score + 1;
    }

    questionCount = questionCount + 1;
    deselectAll();
    if (questionCount < 10) {
        setTimeout(loadQuestion, 2000);
    } else {
        showscore.innerHTML = `
        <h3>Your Score: ${score}/10 </h3>
        <a class="again" href="options.html"> play Again?</a>
        `;
        showscore.classList.remove("scoreArea");
    }
})


window.onload = sendApiRequest
async function sendApiRequest() {
    let response = await fetch(categorylink[level - "0"][category]);
    console.log(categorylink[level - "0"][category]);
    data = await response.json();
    loadQuestion();
}
