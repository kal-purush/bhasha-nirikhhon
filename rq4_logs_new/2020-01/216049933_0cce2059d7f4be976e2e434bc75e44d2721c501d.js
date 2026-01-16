const canvas = document.querySelector('.canvas');
const ctx = canvas.getContext("2d");
const scale = 10;
const rows = canvas.height / scale;
const columns = canvas.width / scale;
let diff = "easy"
let speed;


const diffBtns = document.querySelector(".diff")

diffBtns.addEventListener("click", ()=>{
    diffBtns.style.display = "none";

    if (event.target.value === "easy"){
        speed = 200;
    }

    if (event.target.value === "medium"){
        speed = 100;
        diff = "medium"
    }

    if (event.target.value === "hard"){
        speed = 50;
        diff = "hard"
    }

    if (event.target.value === "impossible"){
        speed = 10;
        diff = "impossible"
    }
    
    setup()
})


function setup(){
    snake = new Snake();
    fruit = new Fruit();

    fruit.pickLocation()
    

    window.setInterval(()=>{
        ctx.clearRect( 0, 0, canvas.height, canvas.height )
        fruit.draw();
        snake.update();
        snake.draw();

        const score = document.querySelector(".score");
            score.innerText = snake.total;

        const scoreBoard = document.querySelector(".score-board");
        const lastScore = localStorage.getItem('score')
            scoreBoard.innerText = `Last Game's Score: ${lastScore} `

        if(diff === "medium"){
            score.innerText = snake.total * 2;
        }

        if(diff === "hard"){
            score.innerText = snake.total * 6;
        }

        if(diff === "impossible"){
            score.innerText = snake.total * 12;
        }

        if(snake.eat(fruit)) {
            fruit.pickLocation();
        }

        if (snake.checkForCollision()){
            window.alert(" Oh no! ");
            localStorage.setItem('score', score.innerText)
            location.reload()
        }

    }, speed)
};

window.addEventListener('keydown', ((e)=>{
    const direction = e.key.replace('Arrow', '')
    snake.changeDirection(direction)
}))