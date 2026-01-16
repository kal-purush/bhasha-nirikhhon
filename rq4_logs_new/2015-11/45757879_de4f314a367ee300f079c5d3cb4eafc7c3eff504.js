var express = require('express');
var bodyParser = require('body-parser');
var elasticsearch = require('elasticsearch');

var client = elasticsearch.Client();
var app = express();
app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());

var router = express.Router();
app.use('/api', router);

router.route('/getItemList')
	.post(function(req,resp){
	
		var filter = {};
		var categorySelected = req.body.data;
		if(categorySelected && categorySelected != "all"){
			var termFilter = {
				term : { category : categorySelected }
			}
			filter = termFilter;
		}

		client.search({
			index : 'item',
			type : 'recipe',
			body:{
			    "size": 200,
			    "filter" : filter,
			    "sort": [
			       	{
			          	"id": {
			             	"order": "asc"
			          	}
			       	}
			    ]
			}
		}, function(err, result){
			if(!err){
				var rawList = result.hits.hits;
				var itemList = []
				for(var key in rawList){
					itemList.push(rawList[key]._source)
				}
				resp.json({ data : itemList })
			}
		});
	});

router.route('/getFavouriteItemList')
	.post(function(req,resp){
		var idList = JSON.parse(req.body.data);
		client.search({
			index : 'item',
			type : 'recipe',
			body:{
			    "size": 200,
			    "filter" : {
			    	"terms" : {
			    		"id" : idList
			    	}
			    },
			    "sort": [
			       	{
			          	"name": {
			             	"order": "asc"
			          	}
			       	}
			    ]
			}
		}, function(err, result){
			if(!err){
				var rawList = result.hits.hits;
				var itemList = []
				for(var key in rawList){
					itemList.push(rawList[key]._source)
				}
				resp.json({ data : itemList })
			}
		});
	});
app.use(express.static(__dirname + '/public/build/Calculator', {maxAge : 31556926000} ));
app.listen(3000);
console.log("BnS Calculator is running on port 3000");