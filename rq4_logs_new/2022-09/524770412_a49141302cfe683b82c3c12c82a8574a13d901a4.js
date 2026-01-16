const elm = document.querySelector('.element');

let btn = document.querySelector('.btn');
btn.onclick = function () {
    elm.style.height = '150px';
    elm.style.width = '450px';
}

const frst = document.querySelector('.first');

let btn2 = document.querySelector('.btn2');
btn2.onclick = function () {
    frst.classList.add('back');
    frst.classList.add('text');
    frst.classList.add('work');
}

let btn3 = document.querySelector('.btn3');
btn3.onclick = function () {
    frst.classList.remove('back');
}