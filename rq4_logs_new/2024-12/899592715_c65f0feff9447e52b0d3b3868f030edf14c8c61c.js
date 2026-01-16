let scoreStr=localStorage.getItem('Score');
let score;

resetScore(scoreStr);

function resetScore(scoreStr){
    score = scoreStr ? JSON.parse(scoreStr) : {
        win:0,
        lost:0,
        tie:0,
    };

    score.displayScore = function(){
        return `win=${score.win} , lost=${score.lost} , tie=${score.tie}`;
    };

    showResult();
}

// if(scoreStr !== undefined){
//     score=JSON.parse(scoreStr);
// }else{
//     score = { 
//         win:0,
//         lost:0,
//         tie:0,
//     };
// }

function generateComputerChoice(){
    let randomNumber=Math.random()*3;
    if(randomNumber >0 && randomNumber <=1){
        return 'Rock';
    }else if(randomNumber >1 && randomNumber <=2){
        return 'Paper';
    }else{
        return 'Scissor';
    }
}

function getResult(userMove,computerMove){
    if(userMove==='Rock'){
        if(computerMove==='Rock'){
            score.tie++;
            return 'Match is tie';
        }else if(computerMove==='Paper'){
            score.lost++;
            return 'Computer Won';
        }else{
            score.win++;
            return 'You Won';
        }
    }else if(userMove==='Paper'){
        if(computerMove==='Rock'){
            score.win++;
            return 'you Won';
        }else if(computerMove==='Paper'){
            score.tie++;
            return 'Match Tie';
        }else{
            score.lost++;
            return 'Computer Won';
        }
    }else{
        if(computerMove==='Rock'){
            score.lost++
            return 'Computer won';
        }else if(computerMove==='Paper'){
            score.win++;
            return 'You Won';
        }else{
            score.tie++;
            return 'Match Tie';
        }
    }
}

function showResult(userMove,computerMove,result){  

    localStorage.setItem('Score',JSON.stringify(score));

    document.querySelector('#user-move').innerText=
        userMove ? `You have choosen ${userMove}` : '';

    document.querySelector('#computer-move').innerText=
        computerMove ? `Computer choice is ${computerMove}` : '';

    document.querySelector('#result').innerText= result || '';
    
    document.querySelector('#score').innerText= score.displayScore();
    
}