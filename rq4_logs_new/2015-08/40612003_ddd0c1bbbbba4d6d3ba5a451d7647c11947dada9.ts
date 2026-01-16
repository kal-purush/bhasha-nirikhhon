import http = require('http');
export class Http1{
	_server:http.Server;
	
	onReq(req:http.IncomingMessage,res:http.ServerResponse){
	  	res.writeHead(200, {'Content-Type': 'text/html'}); 
  		res.write('<h1>Node.js</h1>'); 
  		res.end('<p>Hello World</p>'); 
	}
	public Start(port:number){
		this._server = http.createServer(this.onReq).listen(port);
		console.log("http server start on "+port);
	}

	public Close(){
		if(this._server){
			this._server.close();
			console.log("http server closed...");
		}
	}
	static test(){
		var h = new Http1();
		h.Start(80);
		setTimeout(function(){
			h.Close();
		},10000);
	}
}