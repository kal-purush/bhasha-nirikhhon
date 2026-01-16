var express = require('express');
var fs = require('fs');
var request = require('request');
var cheerio = require('cheerio');
var bodyParser = require('body-parser');
var app     = express();

app.use(bodyParser.urlencoded({ extended: true }));
app.use(bodyParser.json());
var port     = process.env.PORT || 8080; // set our port

// create our router
var router = express.Router();

// middleware to use for all requests
router.use(function(req, res, next) {
	// do logging
	console.log('Something is happening.');
	next();
});

// test route to make sure everything is working (accessed at GET http://localhost:8080/api)
router.get('/', function(req, res) {
	res.json({ message: 'hooray! welcome to our api!' });
});

router.get('/scrape', function(req, res) {
  var data = "";
  url = 'http://www.blocket.se/malmo?q=cykel&cg=0&w=1&st=s&c=&ca=23_11&is=1&l=0&md=th';
  request(url, function(error, response, html){

      // First we'll check to make sure no errors occurred when making the request
      var data = "";
      if(!error){

          // initializse where I keep the items
          var webItems = [];

          // alternative
          var $ = cheerio.load(html, {normalizeWhitespace: true});
          $('.item_row').each(function(i, elem) {
            var bicID = $(this).attr('id');
            var bicDescription = $(this).find($('.media-body')).find($('.h5')).text();
            var longdesc = $(this).find($('img')).attr('longdesc');
            var imgSrc = $(this).find($('img')).attr('src');


            //console.log("long:", longdesc, " img:", imgSrc);
            webItems[i]={'bicID:':bicID,'bicDescription':bicDescription, 'imgSrc':imgSrc, 'longdesc':longdesc}
          });


          res.json(webItems);


          // Finally, we'll define the variables we're going to capture

      }
  })

	//res.json({ message: 'hooray! welcome to our scrape route!', bdata:data });

});


// REGISTER OUR ROUTES -------------------------------
app.use('/api', router);

// START THE SERVER
// =============================================================================
app.listen(port);
console.log('Magic happens on port ' + port);


exports = module.exports = app;