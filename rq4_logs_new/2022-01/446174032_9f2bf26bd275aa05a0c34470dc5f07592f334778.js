const button = document.querySelector('.generator__button')
const includeUppLetter = document.querySelector('.generator__input-upp')
const includeLowLetter = document.querySelector('.generator__input-low')
const includeNumber = document.querySelector('.generator__input-num')
const includeSymbol = document.querySelector('.generator__input-symbol')
const copyData = document.querySelector('.generator__copy')


const insertRandomData = document.querySelector('.generator__data')

let countSymbol = document.querySelector('.generator__input')





const str = 'abcdefghijklmnopqrstuvwxyz'
const num = '0123456789'
const symbol = '!@"№;$%^:&?*()-+_'

let s1 = ''
let s2 = ''
let s3 = ''
let s4 = ''

let words = ''
let random = ''
let copy = ''
let number = 0

number = countSymbol.value

document.addEventListener('change', (event) => {

  // console.log(event.target.checked)
  if (includeUppLetter.checked) {

      s1 = str.toUpperCase()
  } else {
      s1 = ''
      console.log('s1 - else')
  }

  if(includeLowLetter.checked) {
      s2 = str

  } else {
     s2 = ''
  }
  
  if(includeNumber.checked) {
    s3 = num
  } else {
    s3 = ''
  } 
  
  if(includeSymbol.checked) {
    s4 = symbol
  } else {
    s4 = ''
  }

})



countSymbol.addEventListener('change', (event) => {
  console.log(event.target.value)
  number = event.target.value
})


button.addEventListener('click', () => {

  // let countSymbol = document.querySelector('.generator__input').value
  let len = (s1.length+s2.length+s3.length+s4.length)

  random = ''

  for(let i = 0; i < number; i++) {

    random += (s1+s2+s3+s4).charAt(Math.floor(Math.random() * len))

  }

  insertRandomData.innerHTML = random
  
})



copyData.addEventListener('click', () => {
  if(insertRandomData.textContent.length > 0) {
    copy = insertRandomData.textContent
    alert('Пароль скопирован')
    console.log('Пароль скопирован -> ',copy)
  }


})