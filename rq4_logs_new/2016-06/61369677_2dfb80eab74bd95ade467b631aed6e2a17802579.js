'use strict';
var query = '';
var operand = '';

var elements = document.getElementsByTagName('button');

function onBtnClick(event){
    var btn = event.target,
        operator = !!btn.dataset.operator,
        value = btn.dataset.value;

    if (!operator) {
        operand += value;
    } else {

        if (operand){
            if (operator === 'result'){
                console.log('eval:', eval(query));
                query = '';
                operator = '';
                operand = '';
                return;
            } else {
                query += operand + operator;
            }

            operand = '';
        } else {
            query = query.slice(0, query.length - ) + operator;
        }
    }
    console.log(query);
    console.log(operand);

    query += '+' + event.target.dataset.value;
}

for (var i = 0; i < elements.length; i++){
    elements[i].addEventListener('click', onBtnClick);
}