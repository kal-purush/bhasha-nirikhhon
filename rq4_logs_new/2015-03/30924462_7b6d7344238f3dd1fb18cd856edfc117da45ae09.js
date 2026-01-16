var path = require('path');
var qs = require('querystring');

var Handlers = {
	getContentType: function (filename) {
		var ext = path.extname(filename).toLowerCase();
		switch (ext){
			case '.js': return 'application/javascript';
			case '.css': return 'text/css';
			case '.html': return 'text/html';
			default: return 'text/plain';
		}
	},
	post: function (req, res) {
		console.log(req.method);
		var formData = '';
		req.on('readable', function () {
			var data = req.read();

			if(typeof data == 'string'){
				formData += data;
			}
			else if(typeof data == 'object' && data instanceof Buffer){
				formData += data.toString('utf8');
			}
		});

		req.on('end', function () {
			var out = '';
			if (!formData) {
				out = 'No form data';
			} else {
				var obj = qs.parse(formData);
				if(!obj){
					out = 'bad form data';
				} else{
					// var friend = '\n';
					// for(var key in obj){
					// 	friend += obj[key]+' ';
					// }
					out = JSON.stringify(obj);
				}
			}

			res.end(out);
		});
	}
};

module.exports = Handlers;