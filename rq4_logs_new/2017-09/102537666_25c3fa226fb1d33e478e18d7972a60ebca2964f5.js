'use strict';

const request = require('request');
const cheerio = require('cheerio');
const fs = require('fs');

const url = process.argv[2];

getGuild(url);

function getGuild(url) {
	request(url, (error, response, html) => {
	    if (error) {
	        console.error("Error loading url", error);
            process.exit(1);
            return;
	    }
	
	    let $ = cheerio.load(html);
	
        let guildName = $('.character-list ul li:first-of-type h1').clone().children().remove().end().text().trim();
        let uri = $('.character-list ul li:first-of-type h1').find('img').prop('src');
        let name = uri.split('/').slice(-1)[0];
    
        var download = function(uri, filename, callback) {
            request.head(uri, function(err, res, body) {
                request(uri).pipe(fs.createWriteStream('../../public/images/' + filename)).on('close', callback);
            });
        };
        
        download(uri, name, () => {
            console.log(guildName + ':|:' + name);
        });
	});
}