'use strict';

const body = document.querySelector('body'),
    title = document.getElementById('color'),
    button = document.querySelector('#change');

function random(number) {
    return Math.floor(Math.random()*(number+1));
}

button.addEventListener('click', function() {
    let rndColor = 'rgb('+ random(255) + ',' + random(255) + ',' + random(255) +')';
    body.style.backgroundColor = rndColor;

    button.style.color = body.style.backgroundColor;

    let result = rndColor.match(/^rgb\((\d+),\s*(\d+),\s*(\d+)\)$/);;
    function hex(x) {
        return ("0" + parseInt(x).toString(16)).slice(-2);
    }

    title.textContent= "#" + hex(result[1]) + hex(result[2]) + hex(result[3]);
});


