exports.include = (app) => {
	require('./database.js');

	app.get('/lessons/application/', function(request, response) {
	  response.render('lessonApplication/index');
	});

	/*app.post('/register/student/', function(request, response) {
		// DATABASE CONNECTION
		app.client.query("SELECT * FROM test;")
		.on('row', function(row) {
		    console.log(row);
		});
		var student = request.body;
		//response.send('Student Registered');
		response.sendStatus('201');
		//response.sendStatus('500');
	});*/

	app.get('/register/student/*', function(request, response) {
	  response.render('lessonApplication/index');
	});
}