var request = require("request"),
	cheerio = require("cheerio"),
	fs = require("fs");

function UrlContentParser() {
    var self = this;

    self.url = "http://catalog.onliner.by/search";
    self.query = "?query=";

    self.getUrlContent = function (prod_name, callback) {
        request.get({
            headers: { 'content-type': 'text/html' },
            url: self.url + self.query + prod_name
        }, function (error, response, body) {
        	if (error) {
        		cons.log("Eror: %s", error)
        		return callback(eror);
        	}

        	return self.getElementContent(body, callback);
        });
    };

    self.getElementContent = function (data, callback) {
    	var $ = cheerio.load(data);

    	var $content = cheerio.load("<div id='cover'></div>");

    	$('body').find('td.textmain tr').each(function () {
    		if ($(this).find(".pdescr").length) {
    			var title = $(this).find(".pdescr .pname a").text(),
					price = $(this).find(".poffers .pprice a").text();

    			if (price == "") {
    				price = "No Price"
    			}

    			$content('#cover').append("<p><strong>" + title + "</strong>: " + price + "</p>");
    		}
    	});

    	callback(null, $content('#cover').html());
    };
};

module.exports = new UrlContentParser();