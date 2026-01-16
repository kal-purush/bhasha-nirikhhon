const scorebord = document.querySelector('.heighest-score .score')
const deletebtn = document.querySelector('.delete-data')
const deletenotification = document.querySelector('.delete-notification p')
const startbtn = document.querySelector('.start .text p')
const nextbtn = document.querySelector('.start a')
const pageno = Number(localStorage.getItem('pageno'))


const heighestscore = Number(localStorage.getItem('HighestScore'))
if(pageno){
    scorebord.innerHTML = 'Not completed the game'
    startbtn.innerHTML = `Question ${pageno} >>`
    nextbtn.href = `question${pageno}.html`

}
else if(heighestscore){
    scorebord.innerHTML = `${heighestscore}/10`
}
else{
    scorebord.innerHTML = 'New Player'
}

deletebtn.addEventListener('click', function(){
    localStorage.setItem('HighestScore', '0')
    localStorage.setItem('isplayed', 'false')
    localStorage.setItem('isMuted', 'false')
    localStorage.setItem('pageno', '0')
    localStorage.q1visited = localStorage.q2visited = localStorage.q3visited = localStorage.q4visited = localStorage.q5visited = localStorage.q6visited = localStorage.q7visited = localStorage.q8visited = localStorage.q9visited = localStorage.q10visited = 'false';
    deletenotification.style.display = "block"
    scorebord.innerHTML = 'New Player'
    setTimeout(() => {
        deletenotification.style.display = "none"
    }, 1000);
})