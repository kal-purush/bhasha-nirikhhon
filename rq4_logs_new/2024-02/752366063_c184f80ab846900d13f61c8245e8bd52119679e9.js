let $ = document

const plus = $.querySelector('#plus')
const minus = $.querySelector('#minus')
const percent = $.querySelector('#percent')
const times = $.querySelector('#times')
const division = $.querySelector('#division')
const auditor = $.querySelector('#auditor')
const equal = $.querySelector('.equal')
const display = $.querySelector('input')

plus.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '+'
    }
})
minus.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '-'
    }
})
percent.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '%'
    }
})
times.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '*'
    }
})
division.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '/'
    }
})
auditor.addEventListener('click', () => {
    if (display.value !== '') {
        display.value += '.'
    }
})

equal.addEventListener('click', () => {
    if (display.value === '') {
        display.value = ''
    } else {
        display.value = eval(display.value)
    }
})

window.addEventListener('keydown', (event) => {
    console.log(event);
    if (event.key === '1') {
        display.value += '1'
    } else if (event.key === '2') {
        display.value += '2'
    } else if (event.key === '3') {
        display.value += '3'
    } else if (event.key === '4') {
        display.value += '4'
    } else if (event.key === '5') {
        display.value += '5'
    } else if (event.key === '6') {
        display.value += '6'
    } else if (event.key === '7') {
        display.value += '7'
    } else if (event.key === '8') {
        display.value += '8'
    } else if (event.key === '9') {
        display.value += '9'
    } else if (event.key === '0') {
        display.value += '0'
    } else if (event.key === '*' && display.value !== '') {
        display.value += '*'
    } else if (event.key === '-' && display.value !== '') {
        display.value += '-'
    } else if (event.key === '+' && display.value !== '') {
        display.value += '+'
    } else if (event.key === '.' && display.value !== '') {
        display.value += '.'
    } else if (event.key === '/' && display.value !== '') {
        display.value += '/'
    } else if (event.key === '%' && display.value !== '') {
        display.value += '%'
    } else if (event.keyCode === 13 && display.value !== '') {
        display.value = eval(display.value)
    } else if (event.key === 'Backspace') {
        display.value = display.value.toString().slice(0, -1)
    }

})