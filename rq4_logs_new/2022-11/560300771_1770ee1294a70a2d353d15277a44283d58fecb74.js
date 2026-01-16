// Даны 3 инпута и кнопка. По нажатию на кнопку получите числа, стоящие в этих инпутах и запишите их сумму в четвертый инпут

const button = document.getElementById('but')
const inputs = document.getElementsByClassName('inp')
const answer = document.getElementById('answ')
const arr = document.querySelectorAll('.inp')

button.addEventListener('click', ()=> {
    let nums = 0
    for(let input of inputs) {
        nums = nums + (+input.value)
    }
    answer.innerText = nums


    let answ = (nums[0] + nums[1] + nums[2])
    answer.value = answ
})

// Дан инпут. В него вводится число. По потери фокуса найдите сумму цифр этого числа.

const input = document.getElementById('inp')
const answer = document.getElementById('answ')

input.addEventListener('blur', () =>{
    let ans = 0
    const a = input.value.split('')
    for(let num of a) {
        ans = ans + +num
    }
    answer.innerText = ans
    })

// Дан инпут. В него вводятся числа через запятую. По потери фокуса найдите среднее арифметическое этих чисел (сумма делить на количество)

const input = document.getElementById('inp')
const answer = document.getElementById('ans')

input.addEventListener('blur', () => {
    let sum = 0
    const nums = input.value.split(',')
    for(let num of nums) {
        sum += +num
    }
    answer.innerText = +sum / nums.length
})

// Дан инпут. В него вводится ФИО через пробел. По потери фокуса запишите фамилию, имя и отчество в отдельные инпуты.
const input = document.getElementById('inp')
const name = document.getElementById('inpName')
const lastName = document.getElementById('inpLast')
const fathName = document.getElementById('inpFath')

input.addEventListener('blur', () => {
    let fullname = input.value.split(' ')
        name.value = fullname[0]
        lastName.value = fullname[1]
        fathName.value = fullname[2]
})

//Дан инпут. В него вводится ФИО через пробел. ФИО вводится с маленькой буквы. Сделайте так, чтобы по потери фокуса инпутом, введенные фамилия, имя и отчество автоматически стали записанными с большой буквы (в том же инпуте)


const input = document.getElementById('inp')
input.addEventListener('blur', ()=>{
    let liters = ''
    const array = input.value.split(' ')
    for(let name of array) {
        liters += name[0].toUpperCase() + name.slice(1)
    }
    input.value = liters
})

// Дан инпут. В него вводится текст. По потери фокуса узнайте количество слов в этом тексте.

const input = document.getElementById('inp')
input.addEventListener('blur', ()=>{
    let sum = 0
    const array = input.value.split(' ')
    sum = array.length-1
    console.log(sum)
})

// Дан инпут. В него вводится текст. По потери фокуса узнайте количество символов в самом длинном слове в этом тексте.

const input = document.getElementById('inp')
input.addEventListener('blur', ()=>{
    const array = input.value.split(' ')
    let maxLiters = 0
    for(let liter of array) {

        if(liter.length > maxLiters){
            maxLiters = liter.length
        }
    }
    console.log(maxLiters)

    })

// Дан инпут. В него вводится дата в формате 31.12.2016. По потери фокуса в этом же инпуте поставьте эту дату в формате 2016-12-31.
const input = document.getElementById('inp')

input.addEventListener('blur', ()=>{
    const array = input.value.split('.').reverse()
    input.value = array.join('-')
})

Дан инпут. В него вводится год рождения пользователя. По нажатию на кнопку выведите в абзац ниже сколько пользователю лет.

const input = document.getElementById('inp')
const but = document.getElementById('but')
const answ = document.getElementById('answ')
let date = new Date()
let thisYear = date.getFullYear()
but.addEventListener('click', ()=>{
    let birthYear = input.value
    let year = thisYear - +birthYear
    answ.innerText = year
})



// Дан инпут. В него вводится дата в формате 31.12.2016. По потери фокуса узнайте день недели (словом), который приходится на эту дату.

const input = document.getElementById('inp')
const answ = document.getElementById('answ')
input.addEventListener('blur', ()=>{
    let array = input.value.split('.')
    let day = +array[0]
    let month = +array[1] - 1
    let year = +array[2]
    let date = new Date(year, month, day)
    let a = date.getDay()
    if (a === 2) {
        answ.innerText = 'вторник'
        console.log(answ.innerText)
    }

})

// Дан инпут. В него вводится слово. По нажатию на кнопку проверьте то, что это слово читается с начала и с конца одинаково (например, мадам).

const input = document.getElementById('inp')
const button = document.getElementById('but')
const span = document.getElementById('answ')

button.addEventListener('click', ()=>{
    let array = input.value.split('')
    let array2 = input.value.split('')
    let array3 = array2.reverse()
    if (JSON.stringify(array) === JSON.stringify(array3)) {
        span.innerText = 'Ес!'
    } else {
        span.innerText = 'Оу ноу'
    }
})

// Дан инпут. В него вводится число. Проверьте по вводу, что это число содержит внутри себя цифру 3.

const input = document.getElementById('inp')

input.addEventListener('input', ()=>{
    let array = input.value
    for(let tres of array) {
        if (+tres === 3) {
            alert('It\'s three!')
        }
    }
})

// Даны N абзацев и кнопка. По нажатию на кнопку запишите в конец каждого абзаца его порядковый номер.


const but = document.getElementById('but')
const text = document.getElementsByClassName('text')
const text2 = document.querySelectorAll('.text')
console.log(text)
console.log(text2)

but.addEventListener('click', ()=>{
    let a = 0
    for(let span of text){
        text[a].innerText = text[a].innerText.concat(`${++a}`)
    }
})


// Даны N абзацев с числами. По нажатию на кнопку выведите эти числа в инпут через запятую в порядке возрастания.

    const but = document.getElementById('but')
    const text = document.getElementsByClassName('text')
    const inp = document.getElementById('inp')

but.addEventListener('click', ()=>{
        let array = []
    for(let num of text){
        array.push(+num.innerText)
    }
        function textSort(a, b) {
            if (a > b) {
                return 1;
            } else if (b > a) {
                return -1;
            } else {
                return 0;
            }
        }
    inp.value = array.sort(textSort).join(',')
})



// Даны ссылки. По загрузке страницы добавьте в конец каждой ссылки ее href в круглых скобках.

const refs = document.getElementsByClassName('ref')
for(let ref of refs) {

    ref.innerText = ref.innerText + `(${ref.getAttribute('href')})`
    console.log(ref.getAttribute('href'))
}

Даны ссылки. По загрузке страницы, если ссылка начинается с http://, то добавьте ей в конец стрелку →

const refs = document.getElementsByClassName('ref')
for(let ref of refs) {
    if (ref.getAttribute('href').includes('http')) {
        ref.innerText = ref.innerText + '→'
        console.log(ref.innerText)

    }
}

// Даны N абзацев с числами. По нажатию на любой абзац запишите в него квадрат числа, которое в нем находится.

const nums = document.getElementsByClassName('text')
for(let num of nums) {
    num.addEventListener('click', function math() {
        num.innerText = num.innerText + '(' + Math.pow(+num.innerText, 2) + ')'
        num.removeEventListener('click', math)
    })
}



// Даны картинки. По нажатию на любую картинку увеличьте ее в 2 раза.

const but = document.getElementById('but')
const img = document.getElementById('cat')
img.addEventListener('click', func)


function func () {
    img.width = +img.width * 2
    img.removeEventListener('click', func)
    img.addEventListener('click', func2)
}

function func2 () {
    img.width = img.width / 2
    img.removeEventListener('click', func2)
    img.addEventListener('click', func)
}



// Даны N картинок размера 30px. По нажатию на картинку под ними эта картинка появляется размером в 50px

const but = document.getElementById('but')
const img = document.getElementById('cat')
const secimg = document.getElementById('secimg')

img.addEventListener('click', func)

function func() {
    img.removeEventListener('click', func)

   let newImg = img.cloneNode()
    newImg.width = 50
    secimg.append(newImg)

}

// Дан инпут. Реализуйте кнопочки +1, -1, которые будут увеличивать или уменьшать на 1 значение инпута. Сделайте так, чтобы это значение не могло стать меньше нуля.

    const inp = document.getElementById('inp')
    const but1 = document.getElementById('but1')
    const but2 = document.getElementById('but2')

but1.addEventListener('click', ()=> {
    inp.value = +inp.value + 1
})

but2.addEventListener('click', ()=>{
    inp.value = +inp.value - 1
    if (+inp.value < 0) {
        alert('ТРЕВОГА')
    }
})

// Дан инпут. В него вводится число. По потери фокуса проверьте, что в нем лежит число от 1 до 100. Если это так - покрасьте инпут в зеленый цвет, а если не так - в красный
const inp = document.getElementById('inp')

inp.addEventListener('blur', ()=> {
    if (+inp.value >= 1 && +inp.value <= 100) {
        inp.style.color = '#00ff00' }
    else {
        inp.style.color = '#ff0000'
    }
})

Дан инпут. Выделите любой текст на странице. По окончанию выделения этот текст должен записаться в этот инпут.
const inp = document.getElementById('inp')
const span = document.getElementsByClassName('span')

document.addEventListener('mouseup', ()=>{
    inp.value = window.getSelection()
})


// Даны абзацы с числами. По нажатию на кнопку найдите абзац, в котором хранится максимальное число, и сделайте его красного цвета

const inp = document.getElementById('inp')
const nums = document.getElementsByClassName('nums')
const but = document.getElementById('but1')

but.addEventListener('click', ()=> {
    let a = 0
    let maxP = {}
    for(let num of nums) {

        if(+num.innerText >= a) {
            a = +num.innerText
            maxP = num
        }}
    maxP.style.color = 'red'
})

// Дан инпут. Даны абзацы. Пусть в этот инпут записывается суммарное количество нажатий по этим абзацам.


const inp = document.getElementById('inp')
const text = document.getElementsByClassName('text')

let a = 0
text.addEventListener('click', ()=>{
    a += 1
    inp.value = a
})

// Дан инпут с числом. Сделайте так, чтобы каждую секунду в нем появлялся квадрат того числа, которое в нем записано.

const inp = document.getElementById('inp')


setInterval (func, 1000)
function func () {
    let a = inp.value
    let b = a * a
    inp.value = b
}


// Дан инпут и кнопка. По нажатию на кнопку сгенерируйте случайную строку из 8-ми символов и запишите в инпут.

const inp = document.getElementById('inp')
const but = document.getElementById('but1')

const  possible = 'abcdefgefiklmnopqrstxyz'

but.addEventListener('click', ()=> {

    let a =''
    for (let i = 0; i<8; i++) {
        a += func()
    }
    inp.value = a
})

function func () {
    return possible.charAt(Math.floor(Math.random() * (possible.length-1)))

}

// Дан инпут. В него вводится число. По потери фокуса сделайте так, чтобы в абзаце ниже начал тикать обратный отсчет, начиная с введенного числа. Когда отсчет дойдет до нуля - он должен закончится

const inp = document.getElementById('inp')
const timer = document.getElementById('timer')

inp.addEventListener('blur', func)

function func () {
    let a = inp.value
    setInterval(()=>{
        a = +a-1
        timer.innerText = a
        if (a<=0) {
            alert(' достаточно')
            inp.removeEventListener('blur', func)
        }
    }, 1000)
}

// Дан абзац. Сделайте так, чтобы каждую секунду он менял свой цвет с красного на зеленый и наоборот.

const text = document.getElementById('text')
text.style.color = 'green'
setInterval(()=> {
    console.log(text.style.color)
    if (text.style.color == 'green') {
        text.style.color = 'red'}
    else {text.style.color = 'green'}

}, 1000)

// Дан абзац. Дан массив цветов ['red', 'green', 'blue']. Сделайте так, чтобы каждую секунду абзац менял свой цвет на определенное значение их массива: сначала 'red', потом 'green' и так далее. Как только цвета в массиве закончатся - все начнется сначала. Количество цветов в массиве может быть любым.

const text = document.getElementById('text')

let array = ['red', 'green', 'blue']
let a = 0

setInterval(func, 1000)
function func () {
    text.style.color = array[a]
    a++
    if (a == array.length) {
        a = 0
    }
    console.log(text.style.color)
}


// Дан абзац. Дан массив строк ['один', 'два', 'три']. Под абзацем ссылка 'следующая строка'. По заходу на страницу в абзаце должен стоять нулевой элемент этого массива, а по нажатию на ссылку - вставлятся следующий элемент.

const text = document.getElementById('text')
const href = document.getElementById('href')

let array = ['один', 'два', 'три']
text.innerText = array[0]
let a = 0

href.addEventListener('click', function func() {
    text.innerText = array[++a]

    if (a == array.length) {
        href.removeEventListener('click', func)
        achtung('vsye')
    }
})

function achtung(c) {
    alert(c)
}


// Даны инпуты с числами. Произвольное количетсво, пусть три. В первый инпут запишите 1, через секунду во второй инпут запишите 2, еще через секунду в третий инпут 3, потом через секунду в первый инпут запишите 4, во второй 5 и так далее до бесконечности.

    const inp1 = document.getElementById('inp1')
    const inp2 = document.getElementById('inp2')
    const inp3 = document.getElementById('inp3')

    let array = [inp1, inp2, inp3]
let a = 0
let b = 0

    setInterval(func, 1000)
   function func() {
       array[a++].value = ++b
       if (a == array.length) {
           a = 0
           console.log(a)
       }
   }


// Дана ссылка. Дан чекбокс. По нажатию на ссылку меняйте состояние чекбокса с отмеченного на неотмеченное и наоборот.

    const but = document.getElementById('but1')
    const cb = document.getElementById('cb')

but.addEventListener('click', ()=>{
    cb.checked = !cb.checked
})

// Даны чекбокс. Дана кнопка. По нажатию на кнопку сделайте все чекбоксы отмеченными

const but = document.getElementById('but1')
const cb = document.getElementsByClassName('cb')

but.addEventListener('click', ()=>{
    for (let i=0; i<cb.length; i++){
        cb[i].checked = true
    }
})

// Спросите у пользователя какой язык (html, css, js, php) он знает с помощью радио кнопочек. Выведите этот язык в абзац.

const radio = document.getElementsByClassName('r')
const abz = document.getElementById('abz')

    for (let i=0; i<radio.length; i++)
    radio[i].addEventListener('change', ()=>{
        console.log(radio[i].getAttribute('id'))
            abz.innerText = radio[i].getAttribute('id')
        })


// Спросите у пользователя какие языки (html, css, js, php) он знает с помощью чекбоксов. Выбранные языки должны выводится в абзац ниже через запятую.

    const cb = document.getElementsByClassName('r')
    const abz = document.getElementById('abz')

    let array = []
    for (let choice of cb) {
        choice.addEventListener('change', ()=>{
            if (choice.checked === true && !array.includes(choice.getAttribute('id'))) {
                array.push(choice.getAttribute('id'))
                abz.innerText = array.join(', ')
            }
        })

    }


// Дан чекбокс. Дан инпут. Сделайте так, что если чекбокс отмечен - инпут видимый, если не отмечен - не видимый.

const inp = document.getElementById('inp1')
const cb = document.getElementById('cb')
inp.style.opacity = '0'

cb.addEventListener('change', ()=> {
    if (cb.checked === true) {
        inp.style.opacity = '1'
    } else {
        inp.style.opacity = '0'
    }
})

// Даны чекбоксы. Под каждым чекбоксом размещен абзац. Сделайте так, что если чекбокс отмечен - соответствующий абзац видимый, а если не отмечен - не видимый.

const cb = document.getElementsByClassName('r')
const text = document.getElementsByClassName('text')
let elems = document.querySelectorAll('.r + .text')

Array.from(cb).forEach((oneCb, id)=> {
    oneCb.addEventListener('change', () => {
        if (oneCb.checked === true) {
            text[id].style.opacity = '1'
        } else {
            text[id].style.opacity = '0'

        }
    })
})



// Дан инпут. Даны li. В инпут пишется номер. Сделайте так, чтобы по вводу числа, li с заданным номером покрасился в красный цвет.

const inp = document.getElementById('inp')
const lis = document.getElementsByClassName('li')

inp.addEventListener('keyup', func)
function func () {
    for (let li of lis)  {
        if (inp.value === li.innerText) {
            li.style.color = 'red'
        }
    }
}

// Дан абзац. Даны чекбоксы 'перечеркнуть', 'сделать жирным', 'сделать красным'. Если соответствующий чекбокс отмечен - заданное действие происходит с абзацем (становится красным, например). Если чекбоксу снять отметку - действие отменяется.

const text = document.getElementById('text')
const cbs = document.getElementsByClassName('cb')

document.addEventListener('click', ()=>{
    for (let cb of cbs) {
        if (cb.checked) {
            if (cb.getAttribute('id') === 'bold') {
                text.style.fontWeight = 'bold'
            }
            if (cb.getAttribute('id') === 'red') {
                text.style.color = 'red'
            }
            if (cb.getAttribute('id') === 'line') {
                text.style.textDecoration = 'line-through'
            }
        }
    }
})


// Дан блок с кнопкой 'закрыть блок'. По нажатию на эту кнопку блок должен исчезнуть. Кнопка размещается внутри блока и должна исчезнуть вместе с ним. Блоков может быть любое количество, каждый из них закрывает своя кнопка.

const wrap = document.getElementById('wrap')
const but = document.getElementById('but')

but.addEventListener('click', func)
function func() {
    this.parentNode.style.display = 'none'
    console.log(this)
}

// В инпут через запятую вводятся страны. По нажатию на кнопку сделайте так, чтобы эти страны записались в ul под инпутом (каждая страна отдельный li)

const inp = document.getElementById('inp')
const ul = document.getElementById('ul')
const but = document.getElementById('but')

but.addEventListener('click', ()=> {
    const array = inp.value.split(',')
    for (let i of array) {
        let li = document.createElement('li')
        li.innerText = i
        ul.append(li)
    }
})



// На странице есть дивы. В них есть текст. Обойдите все дивы и обрежьте тот текст, который в них стоит так, чтобы он стал длиной 10 символов. И добавьте троеточие в конец обрезанного текста

const divs = document.getElementsByClassName('div')
const but = document.getElementById('but')

but.addEventListener('click', ()=>{
    for (let div of divs) {
            div.innerText = div.innerText.slice(0,9) + '...'
    }
})

// Дана таблица с числами. По нажатию на кнопку найдите ячейку, в которой хранится максимальное число, и сделайте ее фон красным.

const tbl = document.getElementById('tbl')
const but = document.getElementById('but')

but.addEventListener('click', func)
function func() {
    let arr = tbl.getElementsByTagName('td')
    let c = ''
    for (let i of arr) {
        let a = 0
        if (i.innerText >= a) {
            a = i.innerText
            c = i
             }
         }
    c.style.background = 'red'
    }




// Дана таблица с числами. По нажатию на кнопку в инпут под таблицей выведите эти числа через запятую в порядке возрастания.

const tbl = document.getElementById('tbl')
const but = document.getElementById('but')
const inp = document.getElementById('inp')

but.addEventListener('click', func)
function func() {
    let arr = tbl.getElementsByTagName('td')
    let arr1 = []
    for (let i of arr) {
        arr1.push(i.innerText)
    }
    let arr3 = arr1.sort(function(a,b) {
        return a - b;
    })
    inp.value = arr3.join(', ')
}