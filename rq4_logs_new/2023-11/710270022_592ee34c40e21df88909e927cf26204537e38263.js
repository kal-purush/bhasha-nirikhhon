function generateRandom_number(){
    return Math.round(Math.random()*1000);
}

function submit(){
    if (input.value == parseInt(q1.innerText) + parseInt(q2.innerText)){
        correct_status_html.innerText = 'correct';
        corrects++;
    } else {
        correct_status_html.innerText = 'incorrect';
        incorrects++;
    }
    correct_status_html.innerText += `\nyou got ${corrects} correct and ${incorrects} incorrect`

    set_up_question();

}

function set_up_question(){
    input.value = ''
    q1.innerText = generateRandom_number();
    q2.innerText = generateRandom_number();
}

let correct_status_html = document.getElementById('correct_status');
let q1 = document.getElementById('q1');
let q2 = document.getElementById('q2');
let corrects = 0;
let incorrects = 0;
set_up_question();