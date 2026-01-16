'use strict'

const inputRub = document.querySelector('#rub'),
      inputUsd = document.querySelector('#usd');

inputRub.addEventListener('change', () =>{
    const req = new XMLHttpRequest();

    req.open('GET', 'js/current1.json');
    req.setRequestHeader('Content-type', 'application/json', 'charset=utf-8');
    req.send();

    req.addEventListener('load', () =>{
        if(req.status === 200)
        {
            console.log(req.response);
            const data = JSON.parse(req.response);
            inputUsd.value = +inputRub.value / data.current.usd;
        }
        else
        {
            inputUsd.value = "Что-то пошло не так";
        }
    });
});