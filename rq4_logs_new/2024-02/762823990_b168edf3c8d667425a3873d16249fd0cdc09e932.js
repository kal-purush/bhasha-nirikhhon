let imgArr=['./images/dice1.png','./images/dice2.png','./images/dice3.png','./images/dice4.png','./images/dice5.png','./images/dice6.png']



function getRandomDigit(){
let randomNumber1 = Math.floor(Math.random()* (imgArr.length)+1);
document.querySelector('.img1').src = imgArr[Number(randomNumber1)-1]
console.log(randomNumber1)
return randomNumber1
}

function getRandomDigit2(){
randomNumber2 = Math.floor(Math.random()* (imgArr.length)+1);
document.querySelector('.img2').src = imgArr[Number(randomNumber2)-1]
console.log(randomNumber2)
return randomNumber2
}

function getName(randomNumber1 ,randomNumber2){
    if (randomNumber1 > randomNumber2){
        document.querySelector('h1').textContent = 'Player 1 won'
    } else if (randomNumber1 < randomNumber2){
        document.querySelector('h1').textContent = 'Player 2 won'
    } else {
        document.querySelector('h1').textContent = 'Drow'
    }
}




let number_1 = getRandomDigit()

let number_2 = getRandomDigit2()

getName(number_1,number_2)