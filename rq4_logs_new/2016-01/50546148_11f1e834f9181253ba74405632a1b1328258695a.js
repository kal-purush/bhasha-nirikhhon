// link to the connect package
var connect = require('connect');

var url = require('url');

// create a new app using connect
var app = connect();

var calculatorMath = function(req, res, next) {
    // get the subtotal from url's querystring
    var qs = url.parse(req.url, true).query;
    var method = qs.method;
    var x = qs.x;
    var y = qs.y;
    var result
    
    if (qs.method == 'add') {
        method = '+';
    }else if (qs.method == 'subtract') {
		method = '-';
	}
	else if (qs.method == 'multiply') {
		method = '*';
	}
	else if (qs.method == 'divide') {
		method = '/';
	}
	// if method does not equal any of the above, it is invalid
	else {
		method = 'invalid';
	}
    
    if (method === '+') {
       result = parseFloat(x) + parseFloat(y);
    } else if (method === '*') {
        result = parseFloat(x) * parseFloat(y);
    } else if (method === '/') {
        result = parseFloat(x) / parseFloat(y);
    } else if (method === '-'){ 
        result = parseFloat(x) - parseFloat(y);
    } else {
        result = "Error 404 try again later";
    }
    
    res.writeHead(200, {
        "Content-Type": "text-plain"
    });

    // display results
    res.write('16 ' + method + ' 4 = ' + result);

    res.end();
};

// route each url to proper function 
app.use('/calculator', calculatorMath);

//listen for events
app.listen(3000);
console.log('Connect app running at http://localhost:3000');