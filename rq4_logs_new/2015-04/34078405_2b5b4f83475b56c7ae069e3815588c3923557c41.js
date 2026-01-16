// WikiPIZZiA APP

/////////////////////////////////////////////////////////////////// boiler plate
var express = require('express');
var sqlite3 = require('sqlite3');
var fs = require('fs');
var Mustache = require('mustache');
var morgan = require('morgan');
var bodyParser = require('body-parser');
var methodOverride = require('method-override');
var db = new sqlite3.Database('./database.db');
//var path = require('path');
var marked = require('marked');
marked.setOptions({
	renderer: new marked.Renderer(),
	gfm: true,
	tables: true,
	breaks: false,
	pedantic: false,
	sanitize: true,
	smartLists: true,
	smartypants: false
});
//var sendgrid = require("sendgrid")(api_user, api_key);
//var email = new sendgrid.Email();
var app = express();
var he = require('he');
app.use(morgan('dev'));
app.use(bodyParser.urlencoded({extended: false}));
app.use(methodOverride('_method'));
app.use("/styles", express.static(__dirname + '/public/styles'));
var authors = require('./authors.js');
var articles = require('./articles.js');
/////////////////////////////////////////////////////////////////// boiler plate

var articles = {

	showNewArticleForm: function(req, res) {
		var template = fs.readFileSync('./views/newarticle.html', 'utf8');
		//sending new authors to a dropdown menu
		db.all("SELECT * FROM authors;", {}, function(err, authors) {
			var html = Mustache.render(template, {
				allAuthors: authors
			}); //end of mustache
			res.send(html);
		}); //end of db all
	}, //end of showNewArticleForm
	newArticle: function(req, res) {
		var category = req.body.categories;
		var author = req.body.authors;
		db.run("INSERT INTO articles (category, title, content, date_created, image, authors_id) VALUES ('" + category + "','" + req.body.title + "','" + marked(he
			.encode(req.body.content)) + "','" + req.body.date_created + "','" + req.body.image + "','" + author + "')");
		console.log('article info sent to database');
		res.redirect("/articles");
	}, //end of newArticle
	showAllArticles: function(req, res) {
		var template = fs.readFileSync('./views/articles.html', 'utf8');
		var allAuthors = "SELECT * FROM authors;";
		var allArticles = "SELECT * FROM articles;";
		var authorSearch = req.query.authors;
		var authorNameSearchedInArticles = "SELECT * FROM articles WHERE authors_id = " + authorSearch + ";";
		//accessing ALL articles of a particular user
		if (authorSearch) {
			db.all(authorNameSearchedInArticles, {}, function(err, articles) {
				var html = Mustache.render(template, {
					allArticles: articles
				});
				res.send(html);
			}); //end of db all nested
			//accessing ALL articles of ALL users
		} else {
			db.all(allArticles, {}, function(err, articles) {
				var updatedArticles = [];
				var i = 0;
				articles.forEach(function(el) {
					db.each("SELECT * FROM authors WHERE id = " + el.authors_id + ";", {}, function(err, author) {
						console.log(author);
						el.username = author.username;
						updatedArticles.push(el);
						i++;
						if (i === articles.length) {
							console.log(updatedArticles);
							var html = Mustache.render(template, {
								allArticles: updatedArticles
							}); //end of Mustache
							res.send(html);
						} // end of if statement
					}); //end of db each
				}); //end of foreach
			}); //end of db all in else
		} //end of else
	}, //end of showArticles
	showArticle: function(req, res) {
		var id = req.params.id;
		db.all("SELECT * FROM articles WHERE id = " + id + ";", {}, function(err, article) {
			fs.readFile('./views/articlePage.html', 'utf8', function(err, html) {
				var renderedHTML = Mustache.render(html, article[0]);
				console.log(article);
				res.send(renderedHTML);
			});
		});
	}, //end of showOneArticle
	deleteArticle: function(req, res) {
		var id = req.params.id;
		db.run("DELETE FROM articles WHERE id =" + id + ";");
		res.redirect("/articles");
	}, //end of deleteArticle
	saveArticle: function(req, res) {
		var id = req.params.id;
		var articleInfo = req.body;
		db.run("UPDATE articles SET category = '" + articleInfo.category + "', title = '" + articleInfo.title + "', content = '" + marked(he.decode(articleInfo.content)) +
			"', date_created = '" + articleInfo.date_created + "', image = '" + articleInfo.image + "', authors_id = '" + articleInfo.authors_id + "' WHERE id = " +
			id + ";");
		res.redirect("/articles");
	}, //end of saveArticle

}; // end of articles
module.exports = articles;