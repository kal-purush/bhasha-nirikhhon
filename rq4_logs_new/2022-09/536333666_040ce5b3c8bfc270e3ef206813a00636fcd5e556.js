let noGoodBtn = document.getElementById('noGood')
let yesGoodBtn = document.getElementById('yesGood')
let noFastBtn = document.getElementById('noFast')
let yesFastBtn = document.getElementById('yesFast')
let noCheapBtn = document.getElementById('noCheap')
let yesCheapBtn = document.getElementById('yesCheap')
let result = document.getElementById('result')
let select;

function checkBtn(num) {
    if (yesGoodBtn.checked && yesFastBtn.checked && yesCheapBtn.checked) {
        result.style.color = 'red'
        result.style.fontSize = '30px'
        do {
            select = Math.floor(Math.random() * 3 + 1)
        } while (num === select)
        switch (select) {
            case 1:
                noGoodBtn.checked = true;
                result.innerHTML = 'NO WAY'
                break;
            case 2:
                noFastBtn.checked = true;
                result.innerHTML = 'NEVER'
                break;
            case 3:
                noCheapBtn.checked = true;
                result.innerHTML = 'KIDDING ME!!'
                break;
        }
    }
}


function changeGood() {
    yesGoodBtn.checked = true;
    checkBtn(1)
}

function changeFast() {
    yesFastBtn.checked = true;
    checkBtn(2)
}

function changeCheap() {
    yesCheapBtn.checked = true;
    checkBtn(3)
}