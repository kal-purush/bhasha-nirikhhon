const btninc = document.getElementById('increment-btn');
const btndec = document.getElementById('decrement-btn');
const btnres = document.getElementById('reset-btn');
const counterDiv = document.getElementById('counterdiv');

let step = 0;

function increment() {
    let count = document.getElementById('count-el').innerText;
    count++;
    document.getElementById('count-el').innerText = count;
}

function decrement() {
    let count = document.getElementById('count-el').innerText;

    if (count > 0) {
        count--;
        document.getElementById('count-el').innerText = count;
    }
}

function reset() {
    let counterSave = document.createElement('p');
    let counterSaveSpan = document.createElement('span');
    let count = document.getElementById('count-el').innerText;

    let today = new Date();
    let time = today.getHours() + ":" + today.getMinutes() + ":" + today.getSeconds();

    step++;

    let countTime = document.createTextNode(step + '. ' + time + ' = ');
    let counText = document.createTextNode(count);

    counterSaveSpan.appendChild(counText);
    counterSave.appendChild(countTime);
    counterSave.appendChild(counterSaveSpan);
    counterDiv.appendChild(counterSave);

    document.getElementById('count-el').innerText = 0;
}


btninc.addEventListener('click', increment);
btndec.addEventListener('click', decrement);
btnres.addEventListener('click', reset);