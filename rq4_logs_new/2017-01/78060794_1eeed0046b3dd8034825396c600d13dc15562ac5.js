var express = require('express');
var app = express();



app.use(express.static(__dirname + '/public'));

function fibonacci (n) {
    var a = 0, b = 1, f = 1;
    for(var i = 2; i <= n; i++) {
        f = a + b;
        a = b;
        b = f;
    }
    return f;
}



/* Fibonacci */
app.get('/api/Fibonacci',function(request,response){
	var error_INVALID = {"message": "The request is invalid.","error":"-"};

	response.setHeader('Content-Type', 'application/json')

	var n = request.query.n;
	
	var data = {"name":'Fibonacci', "n":n};
	console.log(data);
	
	try {
		if( n <= 1476 && n >= -1476) {
			
			var result = n == 0 ? 0 : fibonacci(Math.abs(n));

			if(n < 0 && n % 2 == 0) {
				result = result * -1;
			}

			console.log('result is '+result);
			response.end(JSON.stringify(result)); 

		}  else {
			response.end(JSON.stringify('no content')); 
		}

	 } catch (e) { 
		error_INVALID.error = e.message;
		response.end(JSON.stringify(error_INVALID)); 
		console.log(e);
	}	
}); 

/* ReverseWords */
app.get('/api/ReverseWords',function(request,response){
	var error_INVALID = {"message": "The request is invalid.","error":"-"};

	response.setHeader('Content-Type', 'application/json')

	var sentence = request.query.sentence;
	
	var data = {"name":'ReverseWords', "sentence":sentence};
	console.log(data);

	if(sentence != undefined) {
		var words = sentence.split(" ");
		var result ='';
		for(var i = 0; i < words.length ; i++) {
			result += words[i].split('').reverse().join('') +' ';
		}

		result = result.substring(0,result.length-1);
		response.end(JSON.stringify(result)); 
	} else {
		response.end(JSON.stringify(''));
	}
	
});



/* Token */
app.get('/token',function(request,response){
	var error_INVALID = {"message": "The request is invalid.","error":"-"};
	response.setHeader('Content-Type', 'application/json')

	var data = {"name":'Token'};
	
	response.end(JSON.stringify('6c94cb55-a665-43f6-9d9e-1461aa5a1ebf'));
});


/* Token */
app.get('/api/token',function(request,response){
	var error_INVALID = {"message": "The request is invalid.","error":"-"};
	response.setHeader('Content-Type', 'application/json')

	var data = {"name":'Token'};
	
	response.end(JSON.stringify('6c94cb55-a665-43f6-9d9e-1461aa5a1ebf'));
});

/* TriangleType */
app.get('/api/TriangleType',function(request,response){
	var error_INVALID = {"message": "The request is invalid.","error":"-"};

	response.setHeader('Content-Type', 'application/json')

	var a = request.query.a;
	var b = request.query.b;
	var c = request.query.c;
	
	var data = {"name":'TriangleType', "a":a, "b":b, "c":c};
	console.log(data);


	if(a != undefined && b != undefined && c != undefined && a > 0 && b> 0 && c >0) {
		
		var angleA = Math.acos((b*b + c*c - a*a)/ (2*b*c) );
		var angleB = Math.acos((- b*b + c*c + a*a)/ (2*a*c) );
		var angleC = Math.acos((b*b - c*c + a*a)/ (2*b*a) );
		
		var sum = parseFloat(angleA +angleB + angleC).toFixed(2) ;
		var pi = parseFloat(Math.PI).toFixed(2);
		console.log('sum of angles = '+ sum + ' - pi =' + pi );
		
		if( sum == pi ) {
			var triangleType = '';

			if(a == b && b == c) {
				triangleType = 'Equilateral';
			} else {
				if(a == b || b == c || c == a) {
					triangleType = 'Isosceles';
				} else {
					triangleType = 'Scalene';
				}
			}
			response.end(JSON.stringify(triangleType));
		} else {
			response.end(JSON.stringify('Error'));
		}
		

		
	} else {

		response.end(JSON.stringify('Error'));
	}

	

 });



/*var server = app.listen(8080, function () {

  var host = server.address().address;
  var port = server.address().port;
  console.log("Example app listening at http://%s:%s", host, port)

})*/

app.use(function(req, res, next) {
  var err = new Error('Not Found');
  err.status = 404;
  next(err);
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;




