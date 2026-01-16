const button = document.querySelector('.advice--button')
const id = document.querySelector('.advice--number')
const advice = document.querySelector('.advice--description')
console.log('hii')
// https://api.adviceslip.com/advice


button.addEventListener('click', () => {
console.log('fetch')
    fetch('https://api.adviceslip.com/advice').then((response) => response.json()).then(data => id.innerText = data.slip.id)
    fetch('https://api.adviceslip.com/advice').then((response) => response.json()).then(data => advice.innerText = data.slip.advice)
} )