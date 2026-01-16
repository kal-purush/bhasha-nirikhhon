let input  = document.querySelector('.input')
let suggestBox = document.querySelector('.suggest-box')
let list = document.querySelectorAll('li')



suggestBox.addEventListener('mouseover', function() {
    suggestBox.classList.add('hovered')
})

suggestBox.addEventListener('mouseout', function() {
    suggestBox.classList.remove('hovered')
})

input.addEventListener('focus', function() {
    suggestBox.style.display = 'flex'
})

input.addEventListener('focusout', function() {
    if(!suggestBox.classList.contains('hovered')) {
        suggestBox.style.display = 'none'
    }
})

list.forEach(function(li) {
    li.addEventListener('click', function() {
        input.value = li.innerText
        suggestBox.classList.remove('hovered')
        suggestBox.style.display = 'none'
    })
})
