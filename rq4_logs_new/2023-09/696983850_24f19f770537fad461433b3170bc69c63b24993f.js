let User_score=document.getElementById('User_score')
let Comp_score=document.getElementById('Comp_score')
let Result=document.getElementById('Result')
let buttons = document.querySelectorAll('.userbtn')
let currentUserscore=Number(User_score.innerText);
let currentCompscore=Number(Comp_score.innerText);
let User_image = document.getElementById('User_image')
let Comp_image = document.getElementById('Comp_image')


const computerchoice = () => {
    const arr = ['Rock', 'Paper', 'Scissors']
    let comchoice = arr[Math.floor((Math.random()) * 3)]
    return comchoice
}

const Score_manage=(userScore,CompScore)=>
{
    currentUserscore+=userScore
    currentCompscore+=CompScore
    User_score.innerText=currentUserscore
    Comp_score.innerText=currentCompscore
    let winner = ( userScore ==0 ) ? (CompScore==0 ? "Draw" : "Computer Won") : ( CompScore==0 ? "User Won": "Invalid");
    Result.innerText=winner
}

buttons.forEach(function (button) {
    button.addEventListener('click', function () {
        let a = computerchoice()
        User_image.innerHTML=`<img src=" Images/${button.value}.png"  class="gameimage" alt="${button.value} Image"></img>`
        Comp_image.innerHTML=`<img src=" Images/Comp_${a}.png" alt="${a} Image" class="gameimage"></img>`
        if (button.value == a) {
            Score_manage(0,0)
        }
        else if (button.value == "Rock" && a == "Scissors") {
            Score_manage(1,0)
            
        }
        else if (button.value == "Paper" && a == "Rock") {
            Score_manage(1,0)
        }
        else if (button.value == "Scissors" && a == "Paper") {
            Score_manage(1,0)
        }
        else {
            Score_manage(0,1)
        }
    });
});



let reset = document.getElementById('reset')
reset.addEventListener('click',function () 
{
    User_score.innerText="0"
    Comp_score.innerText="0"
    currentUserscore=0
    currentCompscore=0
    Result.innerText=""
    User_image.innerHTML="" 
    Comp_image.innerHTML=""
})