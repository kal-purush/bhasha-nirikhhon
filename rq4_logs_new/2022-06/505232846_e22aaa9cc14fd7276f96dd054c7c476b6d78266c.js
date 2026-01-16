// on window onload
window.onload = function () {
    questionContainer.classList.add('hide');
    resultContainer.classList.add('hide');
    enter.addEventListener('click', function (getName) {
        getName.preventDefault();

        // get name from input
        var userName = document.getElementById('user_name').value;
        nameContainer.innerHTML = userName;
        alert(`Welcome ${userName}, Please select a category to start the Quiz`);

        // call category selector function
        categorySelector();
    })
}


// declaring variables

let mainContainer = document.getElementById('main-container');
let questionContainer = document.getElementById('question-container');
let resultContainer = document.getElementById('result-container');
let enter = document.getElementById('name-btn');
let userInputContainer = document.getElementById('user-input-container');
let tryAgain = document.getElementById('try-btn');
const category = document.getElementsByClassName('category-btn');
let categoryName = document.getElementById('category-name');
let question = document.getElementById('question');
let answerBtn = document.getElementsByClassName('answer-btn');
let nextQuestion = document.getElementById('next-btn');
let questionNumber = document.getElementById('question-number');
let timerContainer = document.getElementById('timer-container');
let scoreContainer = document.getElementById('score-container');
let nameContainer = document.getElementById('name-container');
let timeTaken = document.getElementById('time-taken-container');
let attemptedQuestions = document.getElementById('attempt-container');
let correctAnswers = document.getElementById('correct-container');
let wrongAnswers = document.getElementById('wrong-container');
let percentageContainer = document.getElementById('percentage-container');
let homeButton = document.getElementById('home-btn');
var categoryIndex;
var questions;
// create array of categories from HTML collection(category)
let categoryArray = Array.from(category);

// quiz category selector function
function categorySelector() {
    for (let i = 0; i < category.length; i++) {
        category[i].addEventListener('click', function () {
            // categoryIndex = this.id;
            // setting the index of selected category
            categoryIndex = categoryArray.indexOf(category[i]);
            questions = questionsDB[categoryIndex];

            // call start quiz
            startQuiz(categoryIndex);
        })
    }
}

// function to start quiz
function startQuiz(a) {
    // setting value of index to selected category
    i = a;
    // hide user input container
    userInputContainer.classList.add('hide');
    // show(toggle hide) question container
    questionContainer.classList.remove('hide');
    // add quiz category name
    categoryName.innerHTML = category[i].innerHTML;


    // call start timer function to start timer
    // call get question function to get question
    startTimer();
    getQuestion();
}


var clock = 0;
var questionIndex = 0;
var correctAnswer = 0;
var i = 0;
var randomQuestionIndex;
let randomQuestionIndexArray = [];
// quiz timer function
function startTimer() {
    timerContainer.innerHTML = "0";
    // set timer
    var interval = setInterval(function () {
        // timer stops at 300 seconds
        if (clock == 300 || nextQuestion.innerHTML == "See Your Result ") {
            clearInterval(interval);
            timerContainer.innerHTML = "Time's up!";
            timerContainer.style.color = "red";
        }
        else {
            clock++;
            timerContainer.innerHTML = clock;
        }
    }, 1000);
}

// function to get random index number
function randomQuestionIndexGenerator() {
    randomQuestionIndex = Math.floor(Math.random() * questions.length);
    // if random index is already present in array then generate new random index
    if (randomQuestionIndexArray.includes(randomQuestionIndex)) {
        randomQuestionIndexGenerator();
    } else {
        randomQuestionIndexArray.push(randomQuestionIndex);
    }
}

// function to get question
function getQuestion() {
    // call random question function
    randomQuestionIndexGenerator();
    // show question
    
        for (let i = 0; i < answerBtn.length; i++) {
            // remove wrong and right color class from answer buttons
            answerBtn[i].classList.remove('correct');
            answerBtn[i].classList.remove('wrong');
        }
        for (let i = 0; i < answerBtn.length; i++) {
            // add answer to buttons
            answerBtn[i].innerHTML = questions[randomQuestionIndex].options[i];
        }
        question.innerHTML = questions[randomQuestionIndex].problem;
        questionIndex++;
        questionNumber.innerHTML = `${questionIndex}/10`;
        nextQuestion.classList.add('hide');
    
    rightOrWrongAnswer();
}

// next question event listener
nextQuestion.addEventListener('click', showResult);

// show result function
function showResult() {
    mainContainer.style.boxShadow = "0 0 10px 2px black";
    if (nextQuestion.innerHTML == "See Your Result ") {
        questionContainer.classList.add('hide');
        resultContainer.classList.remove('hide');
        // add time taken
        timeTaken.innerHTML = clock;
        // attempted questions
        attemptedQuestions.innerHTML = questionIndex;
        // correct answers
        correctAnswers.innerHTML = correctAnswer;
        // wrong answers
        wrongAnswers.innerHTML = questionIndex - correctAnswer;
        // percentage
        percentageContainer.innerHTML = (correctAnswer / 10) * 100;
    }
    else if (randomQuestionIndexArray.length == questions.length-1) {
        nextQuestion.innerHTML = "Submit";
        getQuestion();
    }
    else if (randomQuestionIndexArray.length == questions.length) {
        nextQuestion.innerHTML = "See Your Result ";
    }
    else {
        getQuestion();
    }
}


// function to iterate through options and on click call selectAnswer function
function rightOrWrongAnswer() {
    for (let i = 0; i < answerBtn.length; i++) {
        answerBtn[i].addEventListener('click', selectAnswer);
    }
}


// selecting answer
function selectAnswer(event) {
    /* if next question button is hidden then only this code will run,
    this will ensure that answer can be selected only once */
    if (nextQuestion.classList.contains('hide')) {
        var selected = event.currentTarget;
        if (selected.innerHTML == questions[randomQuestionIndex].answer) {
            selected.classList.add('correct');
            mainContainer.style.boxShadow = "0 0 10px 2px green";
            correctAnswer = correctAnswer + 1;
            scoreContainer.innerHTML = correctAnswer;
        }
        else {
            selected.classList.add('wrong');
            mainContainer.style.boxShadow = "0 0 10px 2px red";
        }
        nextQuestion.classList.remove('hide');
    }
}

// try again button event listener
tryAgain.addEventListener('click', function () {
    resultContainer.classList.add('hide');
    questionContainer.classList.remove('hide');
    i = 0;
    questionIndex = 0;
    correctAnswer = 0;
    clock = 0;
    scoreContainer.innerHTML = 0;
    nextQuestion.innerHTML = "Next Question";
    randomQuestionIndexArray = [];
    startTimer();
    getQuestion();
})

// home button event listener
homeButton.addEventListener('click', function () {
    location.reload();
})

// question array database
const questionsDB =
    [
        // first set of questions   
        [
            {
                problem: 'Pipes A and B can fill a tank in 5 and 6 hours respectively. Pipe C can empty it in 12 hours. If all the three pipes are opened together, then the tank will be filled in:',
                options: ['30/17 hours', '30/11 hours', '60/17 hours', '9/2 hours'],
                answer: '60/17 hours'
            },
            {
                problem: 'A pump can fill a tank with water in 2 hours. Because of a leak, it took 2 hours to fill the tank. The leak can drain all the water of the tank in:',
                options: ['13/3 hours', '7 hours', '8 hours', '14 hours'],
                answer: '14 hours'
            },
            {
                problem: 'Two pipes A and B can fill a cistern in 37 minutes and 45 minutes respectively. Both pipes are opened. The cistern will be filled in just half an hour, if the B is turned off after:',
                options: ['5 minutes', '9 minutes', '10 minutes', '15 minutes'],
                answer: '9 minutes'
            },
            {
                problem: 'A tank is filled by three pipes with uniform flow. The first two pipes operating simultaneously fill the tank in the same time during which the tank is filled by the third pipe alone. The second pipe fills the tank 5 hours faster than the first pipe and 4 hours slower than the third pipe. The time required by the first pipe is:',
                options: ['6 hours', '10 hours', '15 hours', '30 hours'],
                answer: '10 hours'
            },
            {
                problem: 'Two pipes can fill a tank in 20 and 24 minutes respectively and a waste pipe can empty 3 gallons per minute. All the three pipes working together can fill the tank in 15 minutes. The capacity of the tank is:',
                options: ['60 gallons', '100 gallons', '120 gallons', '180 gallons'],
                answer: '120 gallons'
            },
            {
                problem: 'Two pipes A and B together can fill a cistern in 4 hours. Had they been opened separately, then B would have taken 6 hours more than A to fill the cistern. How much time will be taken by A to fill the cistern separately?',
                options: ['1 hours', '2 hours', '6 hours', '8 hours'],
                answer: '2 hours'
            },
            {
                problem: 'Two pipes A and B can fill a tank in 20 and 30 minutes respectively. If both the pipes are used together, then how long will it take to fill the tank?',
                options: ['12 minutes', '15 minutes', '25 minutes', '50 minutes'],
                answer: '25 minutes'
            },
            {
                problem: 'One pipe can fill a tank three times as fast as another pipe. If together the two pipes can fill the tank in 36 minutes, then the slower pipe alone will be able to fill the tank in:',
                options: ['81 minutes', '108 minutes', '144 minutes', '192 minutes'],
                answer: '192 minutes'
            },
            {
                problem: 'A large tanker can be filled by two pipes A and B in 60 minutes and 40 minutes respectively. How many minutes will it take to fill the tanker from empty state if B is used for half the time and A and B fill it together for the other half?',
                options: ['20 minutes', '15 minutes', '27.5 minutes', '30 minutes'],
                answer: '27.5 minutes'
            },
            {
                problem: 'Three pipes A, B and C can fill a tank in 6 hours. After working at it together for 2 hours, C is closed and A and B can fill the remaining part in 7 hours. The number of hours taken by C alone to fill the tank is:',
                options: ['10 hours', '12 hours', '14 hours', '16 hours'],
                answer: '14 hours'
            }

        ],
        // second set of questions
        [
            {
                problem: 'A box contains 21 balls numbered 1 to 21. A ball is drawn and then another ball is drawn without replacement. What is the probability that both balls are even numbered?',
                options: ['2/7', '8/21', '3/14', '5/21'],
                answer: '3/14'
            },
            {
                problem: 'There are 3 green, 4 orange and 5 white color bulbs in a bag. If a bulb is picked at random, what is the probability of having either a green or a white bulb?',
                options: ['3/4', '2/3', '4/3', '2/5'],
                answer: '2/3'
            },
            {
                problem: 'A box contains slips with numbers from 1 to 50 written on them. A slip is drawn and replaced. Then another slip is drawn and after replacing another slip is drawn. What is the probability that an even number appears on the first draw, an odd number on the second draw and a number divisible by 3 on the third draw? ',
                options: ['1/25', '2/25', '8/25', '4/25'],
                answer: '2/25'
            },
            {
                problem: 'When 4 fair coins are tossed together what is the probability of getting at least 3 heads?',
                options: ['1/4', '3/4', '5/16', '3/8'],
                answer: '5/16'
            },
            {
                problem: 'A committee of 3 members is to be made out of 6 men and 5 women. What is the probability that the committee has at least two women?',
                options: ['10/33', '14/33', '14/15', '13/25'],
                answer: '14/33'
            },
            {
                problem: 'The names of 5 students from section A, 6 students from section B and 7 students from section C were selected. The age of all the 18 students was different. Again, one name was selected from them and it was found that it was of section B. What was the probability that it was the youngest student of the section B? ',
                options: ['1/18', '1/15', '1/6', '1/12'],
                answer: '1/6'
            },
            {
                problem: 'A bag contains 35 balls of three different colors viz. red, orange and pink. The ratio of red balls to orange balls is 3 : 2, respectively and probability of choosing a pink ball is 3/7. If two balls are picked from the bag, then what is the probability that one ball is orange and one ball is pink?',
                options: ['24/119', '60/119', '96/595', '3/17'],
                answer: '24/119'
            },
            {
                problem: 'There are total 18 balls in a bag. Out of them 6 are red in colour, 4 are green in colour and 8 are blue in colour. If Vishal picks three balls randomly from the bag, then what will be the probability that all the three balls are not of the same colour?',
                options: ['95/112', '19/23', '21/26', '46/51'],
                answer: '46/51'
            },
            {
                problem: 'Ram and Shyam are playing chess together. Ram knows the two rows in which he has to put all the pieces in but he doesn’t know how to place them. What is the probability that he puts all the pieces in the right place?',
                options: ['8!/16!', '8!/(2 x 15!)', '8!/15!', '(2 x 8!)/16!'],
                answer: '8!/(2 x 15!)'
            },
            {
                problem: 'A child paints the six faces of a cube with six different colors red, blue, pink, yellow, green and orange. What is the probability that red, pink and blue faces share a common corner?',
                options: ['1/6', '1/20', '1/10', '1/5'],
                answer: '1/5'
            }
        ],
        // third set of questions
        [
            {
                problem: "The ratio of the Mother's age to her daughter's age is 9 : 5. The product of their ages is 1125. The ratio of their ages after five years will be :",
                options: ['1:3', '2:3', '3:4', '5:3'],
                answer: '5:3'
            },
            {
                problem: 'The total ages of Ankit , Narendra and Satendra is 96 years. Five years ago, the ratio of their ages was 2 : 3 : 4. What is the present age of Satendra?',
                options: ['21 years', '32 years', '41 years', '53 years'],
                answer: '41 years'
            },
            {
                problem: "Five years ago the ratio of the ages of Omkar and Nitin was 8 : 7. Three years hence, the ratio of their ages will be 12 : 11. what is Nitin's age at present?",
                options: ['12 years', '15 years', '8.5 years', '19 years'],
                answer: '19 years'
            },
            {
                problem: "The ratio of the present ages of A to that of B is 3 : 5. A's age after 7 years will be 3 years less than the present age of B. What is the sum of the present ages of A's and B's?",
                options: ['10 years', '30 years', '40 years', '45 years'],
                answer: '40 years'
            },
            {
                problem: 'The ratio of the present age of A to that of B is 8 : 9. B is 5 years older than A. After 5 years, what will be the ratio of the age of A to that of B?',
                options: ['1:3', '2:3', '9:10', '10:13'],
                answer: '9:10'
            },
            {
                problem: 'Mohit is younger than Sohit by 4 years. If their ages are in the respective ratio of 3 : 5, How old is Mohit ?',
                options: ['6 years', '12 years', '13 years', '16 years'],
                answer: '6 years'
            },
            {
                problem: 'Mohan was 7 years younger to Raman 5 years back. After 5 years, the ratio of ages of Mohan and Jill will be 3 : 4. The sum of ages of Mohan and Jill is 53 years. Find the current age of Raman.(in years)',
                options: ['22', '24', '29', '34'],
                answer: '29'
            },
            {
                problem: "Miku's age is 9/11th of his brother's age and the age of Miku's father is 23 years more than the age of Miku. If the average age of Miku, Miku's father and Miku's brother is 27 years, find the age of Miku.",
                options: ['18 years', '22 years', '20 years', '15 years'],
                answer: '18 years'
            },
            {
                problem: "Sneha is 8 years older than her cousin. Her cousin is 24 years younger than his mother. If the ratio between the ages of Sneha and her cousin’s mother is 7 : 11. What will be the age of Sneha’s cousin after 3 years?",
                options: ['21 years', '20 years', '26 years', '23 years'],
                answer: '23 years'
            },
            {
                problem: "The ratio of the ages of Esha and her mother is 1 : 4 and the ratio of the ages of Esha's mother and her brother is 9 : 1. If Esha's brother is 5 years younger than Esha. What will be the age of Esha's mother after 4 years?",
                options: ['36 years', '40 years', '45 years', '50 years'],
                answer: '40 years'
            }
        ],
        // fourth set of questions
        [
            {
                problem: 'Rahim marks up all Jeans in his shop 20% higher. He gave 25% discount on 2/5th of the total Jeans and 12% discount on 1/4th of the total Jeans. If Rahim gets an overall profit of 2.3%, then what percentage of discount should be given by Rahim to customers on the remaining Jeans.',
                options: ['5%', '15%', '17%', '6%'],
                answer: '5%'
            },
            {
                problem: "Vipin started a business with an investment of Rs. 42,000. After 5 months Amit joined him with a capital of Rs. 22,000. At the end of the year the total profit was Rs.16,409. What is Vipin’s share in the profit?",
                options: ['Rs. 16244', 'Rs. 12560', 'Rs. 10782', 'Rs. 5677'],
                answer: 'Rs. 12560'
            },
            {
                problem: 'A shopkeeper gives a discount of 10% in every 4 months at an article. If a man purchases it for Rs. 25515 in the month of December, then what was the initial price of that article in the month of January?',
                options: ['Rs. 40000', 'Rs. 36000', 'Rs. 35000', 'Rs. 45000'],
                answer: 'Rs. 35000'
            },
            {
                problem: 'Genelia bought some stationery items at the rate of 5 for Re. 1 and an equal number of items at the rate of 4 for Re. 1. She mixed both the types of items and sold them at the rate of 9 for Rs. 2. In this transaction she bore a loss of Rs. 3. Find the total number of items purchased by her? ',
                options: ['1100', '1080', '1060', '1020'],
                answer: '1080'
            },
            {
                problem: 'A man purchased 150 chairs, each costing the same, but 40% of them are damaged which cannot be sold. He sold 50% of the remaining at 20% profit each and remaining at 5% loss each. If the total selling price of chairs is Rs. 7740, then what was the total cost price of all chairs?',
                options: ['Rs. 13500', 'Rs. 11000', 'Rs. 12000', 'Rs. 15000'],
                answer: 'Rs. 12000'
            },
            {
                problem: 'Anil makes a profit of 18% on cost price by selling a washing machine for Rs. 5900. If the cost price of the machine is increased by 5% and he wants to earn the same profit, What will be the new profit percent on selling price?',
                options: ['14.63%', '12.25%', '15.96%', '17.14%'],
                answer: '14.63%'
            },
            {
                problem: 'After selling a suit for Rs. 1680 a shopkeeper suffers a loss of 16%. If he wants to earn 15% profit after giving the discount of 8%, what will be the marked price?',
                options: ['Rs. 2500', 'Rs. 2000', 'Rs. 1500', 'Rs. 2600'],
                answer: 'Rs. 2500'
            },
            {
                problem: 'A Shopkeeper sells 2 things – bed set and sofa set , both at the same price of Rs 34569. In doing so he manages to gain 10% on bed set while he incurred a loss of 10% in selling sofa set. Then the net percentage profit or loss in whole transaction is? ',
                options: ['1% profit', '11% loss', '1% profit', '1% loss'],
                answer: '1% loss'
            },
            {
                problem: 'The difference between a discount of 25% and two successive discounts of 10% and 20% on a certain bill was Rs. 90. Find the amount of the bill.',
                options: ['Rs. 1500', 'Rs. 300', 'Rs. 3000', 'Data Inadeqate'],
                answer: 'Rs. 3000'
            },
            {
                problem: 'The cost price of 2 jackets and 3 jeans is Rs 3800 and the cost price of 3 jackets and 2 jeans is Rs 3700. If a shopkeeper earns 14% profit on a jacket and 10% profit on a jeans, find the ratio of the selling prices of the jacket to that of jeans.',
                options: ['200:261', '305:396', '315:416', 'None of these'],
                answer: 'None of these'
            }
        ]
    ]; 