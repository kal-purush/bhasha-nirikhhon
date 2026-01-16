var express = require('express');
var app = express();
var mysql = require('mysql');
var bodyParser = require('body-parser');

app.set('view engine', 'ejs');
app.use(bodyParser.urlencoded({ extended: true }));
app.use(express.static(__dirname + '/public'));
var connection = mysql.createConnection({
	host: 'remotemysql.com',
	user: 'ZogkJE9gUn',
	database: 'ZogkJE9gUn',
	password: 'yYG9bt9yJU',
});

let data = [
	['Oprah Winfrey', "If you look at what you have in life, you'll always have more. If you look at what you don't have in life, you'll never have enough."],
    ["James Cameron", "If you set your goals ridiculously high and it's a failure, you will fail above everyone else's success."],
    ["John Lennon", "Life is what happens when you're busy making other plans."],
    ["Mother Teresa", "Spread love everywhere you go. Let no one ever come to you without leaving happier."],
    ["Franklin D. Roosevelt", "When you reach the end of your rope, tie a knot in it and hang on."],
    ["Margaret Mead", "Always remember that you are absolutely unique. Just like everyone else."],
    ["Robert Louis Stevenson", "Don't judge each day by the harvest you reap but by the seeds that you plant."],
    ["Eleanor Roosevelt", "The future belongs to those who believe in the beauty of their dreams."],
    ["Benjamin Franklin", "Tell me and I forget. Teach me and I remember. Involve me and I learn."],
    ["Helen Keller", "The best and most beautiful things in the world cannot be seen or even touched - they must be felt with the heart."],
    ["Aristotle", "It is during our darkest moments that we must focus to see the light."],
    ["Anne Frank", "Whoever is happy will make others happy too."],
    ["Ralph Waldo Emerson", "Do not go where the path may lead, go instead where there is no path and leave a trail."],
    ["Franklin D. Roosevelt", "When you reach the end of your rope, tie a knot in it and hang on."],
    ["Margaret Mead", "Always remember that you are absolutely unique. Just like everyone else."],
    ["Benjamin Franklin", "Tell me and I forget. Teach me and I remember. Involve me and I learn."]]

app.get('/', function(req, res) {
	//var q = 'INSERT INTO quotes (author, content) VALUES ?;';
	var q = 'SELECT * from quotes;'
	connection.query(q, [data], function(error, results) {
		if (error) throw error;
		res.send(results);
	});

	connection.end();
});


app.listen(3000, function() {
	console.log('App is listening on port 3000!');
});