const sum = document.querySelector('.sum')
const result = document.querySelector('.result')
const btn = document.querySelectorAll('.number')
const ac = document.querySelector('.ac')
const plus = document.querySelector('.plus')
const minus = document.querySelector('.minus')
const multiply = document.querySelector('.multiply')
const devide = document.querySelector('.devide')
const equal = document.querySelector('.equal')
const dot = document.querySelector('.dot')

 

function clean () {
        sum.textContent = ""
        result.textContent = ""
}

for (let i = 0;i < btn.length;i++){
    function clickBtn () {
        if(sum.textContent === '' && btn[i] === plus 
        || sum.textContent === '' && btn[i] === multiply
        || sum.textContent === '' && btn[i] === devide){
            alert('Operation invalid, please press correctly')
            return
         }
        const btnNum = btn[i].textContent
        sum.innerHTML += btnNum
        if(sum.textContent.includes('++') || 
        sum.textContent.includes('--') ||
        sum.textContent.includes('**') ||
        sum.textContent.includes('//') || 
        sum.textContent.includes('+-') ||
        sum.textContent.includes('-+') ||
        sum.textContent.includes('+*') ||
        sum.textContent.includes('*+') ||
        sum.textContent.includes('-*') ||
        sum.textContent.includes('*-') ||
        sum.textContent.includes('*/') ||
        sum.textContent.includes('/*') ||
        sum.textContent.includes('..') ||
        sum.textContent.includes('.+') ||
        sum.textContent.includes('+.') ||
        sum.textContent.includes('.-') ||
        sum.textContent.includes('-.') ||
        sum.textContent.includes('.*') ||
        sum.textContent.includes('*.') ||
        sum.textContent.includes('/.') ||
        sum.textContent.includes('./')){
            alert('Operation invalid, please press correctly')
            sum.textContent = sum.textContent.slice(0, -1)
        }
        
    }
    btn[i].addEventListener('click', clickBtn)

    
}

function equalResult (){
    result.innerHTML = eval(sum.textContent)
}

equal.addEventListener('click', equalResult)
ac.addEventListener('click', clean)




// function plusHeader () {
//     let resultNumber = parseInt(result.textContent)
//     let sumNumber = parseInt(sum.textContent)
//     result.innerHTML = resultNumber + sumNumber
//     if(plus){
//         sum.textContent = '+'
//       }
//  }