$(document).ready(function() {
    $.ajax({
        url: "http://52.167.10.58/customer/api/customers",
        headers: { 'Ocp-Apim-Subscription-Key': '9b87fbabbf0447948a3db01212f96d48;product=unlimited' }
    }).then(function(data) {
       $('.clientes').append(data[Math.floor(Math.random()*3)]);
    });
});

/* $(document).ready(function() {
    $.ajax({
        url: "http://52.141.219.120/product/api/products",
        headers: { 'Ocp-Apim-Subscription-Key': '9db32e4fed844af0aac4537ed2f784c8;product=unlimited' }
    }).then(function(data) {
       $('.productos').append(data[Math.floor(Math.random()*3)]);
    });
});*/