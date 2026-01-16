var btn = document.querySelector('#btn');
var span = document.querySelector('#price');


btn.addEventListener('click', function(){
    var XHR = new XMLHttpRequest();

    XHR.onreadystatechange = function(){
        if(XHR.status == 200 && XHR.readyState== 4){
           var money = JSON.parse(XHR.responseText)
           
            var usa = money.bpi.USD.rate;
            var des = money.bpi.USD.description
            span.innerHTML = usa + " " + des;   
        }
    }
    XHR.open('GET', 'https://api.coindesk.com/v1/bpi/currentprice.json');
    XHR.send();
})