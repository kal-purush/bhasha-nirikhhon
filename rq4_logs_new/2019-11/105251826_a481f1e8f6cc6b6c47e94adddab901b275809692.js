var http = require('http');

var server = http.createServer(function(req, res){
	
	 res.writeHead(200,{'content-type': 'text/html'});
	 
	 var getAnimator = fs.readFileSync(__dirname('/')+ 'Animator.html', 'utf8');
	 res.end(getAnimator);
});

server.listen(3000);